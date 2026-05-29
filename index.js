require("dotenv").config();

const express = require("express");
const line = require("@line/bot-sdk");

let supabaseModule = require("./config/supabase");
const supabase = supabaseModule.supabase || supabaseModule;

const {
  createOrder,
  addDriverReport,
  decideWinner,
  getOrderByCodeAndAddress,
  getLatestCustomerOrder,
  upsertCustomerPreference,
  getCustomerPreference,
  resetOrderForReDispatch,
  getOpenOrdersForRefresh,
  markOrderRefreshed,
  cancelLatestCustomerOrder,
  assignWinnerDriver,
  overrideDriver,
  getFirstDriverReport,
  upsertDriverCurrentOrder,
  getBotSetting,
  setBotSetting
} = require("./services/orderService");

const app = express();

if (!process.env.LINE_CHANNEL_ACCESS_TOKEN) throw new Error("Missing LINE_CHANNEL_ACCESS_TOKEN");
if (!process.env.LINE_CHANNEL_SECRET) throw new Error("Missing LINE_CHANNEL_SECRET");
if (!process.env.DRIVER_GROUP_ID) throw new Error("Missing DRIVER_GROUP_ID");

const DRIVER_GROUP_ID = process.env.DRIVER_GROUP_ID;
const DRIVER_GROUP_SOURCE = "A";
const GOOGLE_API_ENABLED = false;

let BOT_ENABLED = true;
let REFRESH_ENABLED = true;

const COMPETE_DIFF_MINUTES = 3;
const OVERRIDE_DIFF_MINUTES = 7;

const REFRESH_INTERVAL_MS = 60000;
const REFRESH_BATCH_SIZE = 1;

const MESSAGE_WORKER_INTERVAL_MS = 2500;
const MAX_RETRY = 5;

const PRIORITY_OVERRIDE_SPRAY = 2;
const PRIORITY_CUSTOMER = 3;
const PRIORITY_COUNTDOWN_SPRAY = 4;
const PRIORITY_NEW_ORDER = 5;
const PRIORITY_REFRESH = 9;

const clients = new Map();
const pendingReservationChanges = new Map();
const processingOrders = new Set();
const refreshingOrders = new Set();
const decidingOrders = new Set();

const customerCooldown = new Map();
const driverCooldown = new Map();

const blacklistCustomers = new Set();
const cancelFees = new Map();

let totalOrders = 0;
let total429 = 0;
let totalCanceled = 0;
let totalAssigned = 0;
let messageWorkerRunning = false;

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function registerClient(sourceName, channelAccessToken, channelSecret) {
  const config = { channelAccessToken, channelSecret };

  clients.set(sourceName, {
    client: new line.Client(config),
    config
  });

  return config;
}

function getClientBySource(sourceName) {
  const item = clients.get(sourceName);
  if (!item) throw new Error(`Unknown LINE source: ${sourceName}`);
  return item.client;
}

const configA = registerClient(
  "A",
  process.env.LINE_CHANNEL_ACCESS_TOKEN,
  process.env.LINE_CHANNEL_SECRET
);

const hasLineB =
  !!process.env.LINE_B_CHANNEL_ACCESS_TOKEN &&
  !!process.env.LINE_B_CHANNEL_SECRET;

let configB = null;

if (hasLineB) {
  configB = registerClient(
    "B",
    process.env.LINE_B_CHANNEL_ACCESS_TOKEN,
    process.env.LINE_B_CHANNEL_SECRET
  );
  console.log("官方B 已啟用");
} else {
  console.log("官方B 尚未啟用");
}

function getErrorStatus(err) {
  return err?.statusCode || err?.originalError?.response?.status;
}

function getErrorData(err) {
  return err?.originalError?.response?.data || err.message;
}

function getArrivalTimeMs(reportTime, minutes) {
  return new Date(reportTime).getTime() + Number(minutes) * 60 * 1000;
}

function detectPaymentMethod(text) {
  const rules = [
    { keywords: ["客下街口", "下街口", "街口"], value: "客下街口" },
    { keywords: ["客下轉帳"], value: "客下轉帳" },
    { keywords: ["轉帳"], value: "轉帳" },
    { keywords: ["現金"], value: "現金" }
  ];

  for (const rule of rules) {
    for (const keyword of rule.keywords) {
      if (text.includes(keyword)) {
        return { keyword, value: rule.value };
      }
    }
  }

  return null;
}

function removePaymentKeyword(text, keyword) {
  return text.replace(keyword, "").replace(/\s+/g, " ").trim();
}

function normalizeReportedAddress(address) {
  return address
    .replace(/客下街口/g, "")
    .replace(/客下轉帳/g, "")
    .replace(/轉帳/g, "")
    .replace(/現金/g, "")
    .replace(/代收取消費\d+/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

async function enqueueMessage({
  toId,
  sourceName = "A",
  priority = 5,
  message,
  orderId = null,
  jobKey = null
}) {
  const payload = {
    to_id: toId,
    source_name: sourceName,
    priority,
    message_json: message,
    status: "pending",
    retry_count: 0,
    next_retry_at: new Date().toISOString(),
    order_id: orderId,
    job_key: jobKey
  };

  const query = supabase.from("message_jobs");

  const { error } = jobKey
    ? await query.upsert(payload, { onConflict: "job_key" })
    : await query.insert(payload);

  if (error) throw error;
}

async function queueRefreshText(to, text, source = "A") {
  if (!REFRESH_ENABLED) return;

  return enqueueMessage({
    toId: to,
    sourceName: source,
    priority: PRIORITY_REFRESH,
    message: { type: "text", text }
  });
}

async function queueGroupMention(
  userId,
  text,
  orderId = null,
  priority = PRIORITY_COUNTDOWN_SPRAY
) {
  return enqueueMessage({
    toId: DRIVER_GROUP_ID,
    sourceName: DRIVER_GROUP_SOURCE,
    priority,
    orderId,
    jobKey: orderId ? `spray:${orderId}:${userId}:${text}` : null,
    message: {
      type: "textV2",
      text: "{driver} " + text,
      substitution: {
        driver: {
          type: "mention",
          mentionee: {
            type: "user",
            userId
          }
        }
      }
    }
  });
}

async function queueTwoMentions(oldUserId, newUserId) {
  return enqueueMessage({
    toId: DRIVER_GROUP_ID,
    sourceName: DRIVER_GROUP_SOURCE,
    priority: PRIORITY_OVERRIDE_SPRAY,
    message: {
      type: "textV2",
      text: "{oldDriver} X\n{newDriver} 噴",
      substitution: {
        oldDriver: {
          type: "mention",
          mentionee: {
            type: "user",
            userId: oldUserId
          }
        },
        newDriver: {
          type: "mention",
          mentionee: {
            type: "user",
            userId: newUserId
          }
        }
      }
    }
  });
}

async function pushCustomerDispatch(customerLineId, plate, minutes, source = "A") {
  return enqueueMessage({
    toId: customerLineId,
    sourceName: source,
    priority: PRIORITY_CUSTOMER,
    message: {
      type: "text",
      text: `司機已出發\n車牌:${plate}\n約${minutes}分鐘`
    }
  });
}

async function pushCustomerArrived(customerLineId, plate, source = "A") {
  return enqueueMessage({
    toId: customerLineId,
    sourceName: source,
    priority: PRIORITY_CUSTOMER,
    message: {
      type: "text",
      text: `車輛已抵達\n車牌:${plate}`
    }
  });
}

async function pushCustomerReservationChanged(customerLineId, reservationTime, source = "A") {
  return enqueueMessage({
    toId: customerLineId,
    sourceName: source,
    priority: PRIORITY_CUSTOMER,
    message: {
      type: "text",
      text: `已為您更改為預約單\n預約時間:${reservationTime}`
    }
  });
}

async function pushAskDriverReservationChange(order, reservationTime, paymentText = "") {
  return enqueueMessage({
    toId: DRIVER_GROUP_ID,
    sourceName: DRIVER_GROUP_SOURCE,
    priority: PRIORITY_NEW_ORDER,
    message: {
      type: "textV2",
      text:
        `${order.order_code} ${reservationTime} ${order.address}${paymentText}\n` +
        "{driver} 可不可更改",
      substitution: {
        driver: {
          type: "mention",
          mentionee: {
            type: "user",
            userId: order.assigned_driver_line_id
          }
        }
      }
    }
  });
}

async function processMessageJobs() {
  if (messageWorkerRunning) return;
  messageWorkerRunning = true;

  let claimed = null;

  try {
    const now = new Date().toISOString();
    const staleTime = new Date(Date.now() - 2 * 60 * 1000).toISOString();

    await supabase
      .from("message_jobs")
      .update({
        status: "pending",
        locked_at: null,
        error_message: "recovered from stuck processing"
      })
      .eq("status", "processing")
      .lt("locked_at", staleTime);

    const { data: jobs, error } = await supabase
      .from("message_jobs")
      .select("*")
      .eq("status", "pending")
      .or(`next_retry_at.is.null,next_retry_at.lte.${now}`)
      .order("priority", { ascending: true })
      .order("created_at", { ascending: true })
      .limit(1);

    if (error) throw error;
    if (!jobs || jobs.length === 0) return;

    const job = jobs[0];

    const { data, error: claimError } = await supabase
      .from("message_jobs")
      .update({
        status: "processing",
        locked_at: new Date().toISOString()
      })
      .eq("id", job.id)
      .eq("status", "pending")
      .select("*")
      .maybeSingle();

    if (claimError) throw claimError;
    if (!data) return;

    claimed = data;

    const targetClient = getClientBySource(claimed.source_name);

    await targetClient.pushMessage(claimed.to_id, claimed.message_json);

    await supabase
      .from("message_jobs")
      .update({
        status: "sent",
        sent_at: new Date().toISOString(),
        locked_at: null,
        error_message: null
      })
      .eq("id", claimed.id);
  } catch (err) {
    const status = getErrorStatus(err);
    const data = getErrorData(err);

    console.error("processMessageJobs error:", data);

    total429 += status === 429 ? 1 : 0;

    if (claimed) {
      const currentRetry = Number(claimed.retry_count || 0) + 1;
      const retryDelayMs = status === 429 ? 60 * 1000 : 15 * 1000;
      const nextRetryAt = new Date(Date.now() + retryDelayMs).toISOString();

      await supabase
        .from("message_jobs")
        .update({
          status: currentRetry >= MAX_RETRY ? "failed" : "pending",
          locked_at: null,
          retry_count: currentRetry,
          next_retry_at: nextRetryAt,
          error_message: typeof data === "string" ? data : JSON.stringify(data)
        })
        .eq("id", claimed.id);
    }
  } finally {
    messageWorkerRunning = false;
  }
}

async function replyText(clientObj, replyToken, text) {
  return clientObj.replyMessage(replyToken, {
    type: "text",
    text
  });
}

async function replyMention(clientObj, replyToken, userId, text) {
  return clientObj.replyMessage(replyToken, {
    type: "textV2",
    text: "{driver} " + text,
    substitution: {
      driver: {
        type: "mention",
        mentionee: {
          type: "user",
          userId
        }
      }
    }
  });
}

function checkCustomerCooldown(customerLineId) {
  const last = customerCooldown.get(customerLineId);

  if (last && Date.now() - last < 5000) {
    return false;
  }

  customerCooldown.set(customerLineId, Date.now());
  return true;
}

function checkDriverCooldown(driverLineId) {
  const last = driverCooldown.get(driverLineId);

  if (last && Date.now() - last < 3000) {
    return false;
  }

  driverCooldown.set(driverLineId, Date.now());
  return true;
}

function parseStrictDriverMessage(text) {
  const clean = text.replace(/\n/g, " ").replace(/\s+/g, " ").trim();
  const parts = clean.split(" ");

  if (parts.length < 4) return null;

  const orderCode = parts[0];
  const lastText = parts[parts.length - 1];
  const plate = parts[parts.length - 2];
  const address = parts.slice(1, parts.length - 2).join(" ");

  if (!orderCode.startsWith("#")) return null;
  if (!address || !plate) return null;

  const minutes = Number(lastText);

  if (Number.isFinite(minutes) && minutes > 0) {
    return {
      type: "report",
      orderCode,
      address,
      plate,
      minutes
    };
  }

  if (["到", "抵達", "到，客直上"].includes(lastText)) {
    return {
      type: "arrived",
      orderCode,
      address,
      plate
    };
  }

  if (["上", "客上", "客人直接上車"].includes(lastText)) {
    return {
      type: "customer_on",
      orderCode,
      address,
      plate
    };
  }

  return null;
}

async function rememberDriverOrder({
  driverLineId,
  order,
  orderCode,
  address,
  plate,
  status = "assigned"
}) {
  return upsertDriverCurrentOrder({
    driverLineId,
    orderId: order.order_id,
    orderCode,
    address,
    plate,
    status
  });
}

async function handleBotControl(event, text, clientObj) {
  if (event.source.type !== "group") return false;

  if (text === "停止機器人運作" || text === "停止") {
    BOT_ENABLED = false;
    await setBotSetting("bot_enabled", "false");
    await replyText(clientObj, event.replyToken, "機器人已停止運作");
    return true;
  }

  if (text === "開始機器人運作" || text === "開始") {
    BOT_ENABLED = true;
    await setBotSetting("bot_enabled", "true");
    await replyText(clientObj, event.replyToken, "機器人已開始運作");
    return true;
  }

  if (text === "停止刷單") {
    REFRESH_ENABLED = false;
    await setBotSetting("refresh_enabled", "false");
    await replyText(clientObj, event.replyToken, "刷單功能已停止");
    return true;
  }

  if (text === "開始刷單") {
    REFRESH_ENABLED = true;
    await setBotSetting("refresh_enabled", "true");
    await replyText(clientObj, event.replyToken, "刷單功能已開始");
    return true;
  }

  if (text === "系統狀態") {
    const { count: pendingJobs } = await supabase
      .from("message_jobs")
      .select("*", { count: "exact", head: true })
      .eq("status", "pending");

    await replyText(
      clientObj,
      event.replyToken,
      `BOT:${BOT_ENABLED}
REFRESH:${REFRESH_ENABLED}
429:${total429}
總訂單:${totalOrders}
已取消:${totalCanceled}
已派送:${totalAssigned}
待送訊息:${pendingJobs || 0}
GoogleAPI:${GOOGLE_API_ENABLED ? "ON" : "OFF"}`
    );

    return true;
  }

  return false;
}

function registerWebhook(path, config, sourceName) {
  app.post(path, line.middleware(config), async (req, res) => {
    res.status(200).end();

    const events = req.body.events || [];
    const item = clients.get(sourceName);

    if (!item) {
      console.error("Unknown webhook source:", sourceName);
      return;
    }

    for (const event of events) {
      handleEvent(event, item.client, sourceName).catch(err => {
        console.error(`${sourceName} error:`, err);
      });
    }
  });
}

registerWebhook("/webhook", configA, "A");

if (hasLineB) {
  registerWebhook("/webhook-b", configB, "B");
}

async function handleEvent(event, clientObj, source) {
  if (event.type !== "message") return;
  if (event.message.type !== "text") return;

  const text = event.message.text.trim();
  if (!text) return;

  const controlled = await handleBotControl(event, text, clientObj);
  if (controlled) return;

  if (event.source.type === "group") {
    if (event.source.groupId !== DRIVER_GROUP_ID) return;
    if (!BOT_ENABLED) return;

    if (text === "可" || text === "不同意") {
      const reservationReply = await handleReservationDriverReply(event, text, clientObj);
      if (reservationReply) return;
    }

    if (!checkDriverCooldown(event.source.userId)) return;
    if (!text.startsWith("#")) return;

    const parsedStrict = parseStrictDriverMessage(text);

    if (!parsedStrict) {
      return replyMention(clientObj, event.replyToken, event.source.userId, "X");
    }

    return handleDriverReport(event, text, clientObj, parsedStrict);
  }

  if (event.source.type === "user") {
    if (!BOT_ENABLED) return;
    return handleCustomerOrder(event, text, clientObj, source);
  }
}

async function handleCustomerOrder(event, addressText, clientObj, source) {
  const customerLineId = event.source.userId;

  if (blacklistCustomers.has(customerLineId)) {
    return replyText(clientObj, event.replyToken, "您目前無法使用叫車");
  }

  if (!checkCustomerCooldown(customerLineId)) {
    return replyText(clientObj, event.replyToken, "請稍後再試");
  }

  try {
    if (processingOrders.has(customerLineId)) return;
    processingOrders.add(customerLineId);

    if (
      addressText === "取消" ||
      addressText === "取" ||
      addressText === "取消叫車" ||
      addressText === "不用車"
    ) {
      const canceledOrder = await cancelLatestCustomerOrder(customerLineId);
      processingOrders.delete(customerLineId);

      if (!canceledOrder) {
        return replyText(clientObj, event.replyToken, "目前沒有可取消的訂單");
      }

      if (canceledOrder.assigned_driver_line_id) {
        await queueGroupMention(
          canceledOrder.assigned_driver_line_id,
          "取",
          canceledOrder.order_id,
          PRIORITY_OVERRIDE_SPRAY
        );
      }

      const currentFee = cancelFees.get(customerLineId) || 0;
      cancelFees.set(customerLineId, currentFee + 100);
      totalCanceled++;

      return replyText(
        clientObj,
        event.replyToken,
        `已取消叫車\n取消費:${cancelFees.get(customerLineId)}`
      );
    }

    if (addressText === "取消付款設定") {
      await upsertCustomerPreference(customerLineId, "");
      processingOrders.delete(customerLineId);
      return replyText(clientObj, event.replyToken, "已取消您的固定付款方式");
    }

    const paymentDetected = detectPaymentMethod(addressText);

    if (paymentDetected) {
      await upsertCustomerPreference(customerLineId, paymentDetected.value);

      const cleanedAddress = removePaymentKeyword(addressText, paymentDetected.keyword);

      if (cleanedAddress.length < 3) {
        processingOrders.delete(customerLineId);
        return replyText(
          clientObj,
          event.replyToken,
          `已記住您的付款方式:${paymentDetected.value}`
        );
      }

      addressText = cleanedAddress;
    }

    if (addressText.startsWith("改預約")) {
      processingOrders.delete(customerLineId);
      return handleCustomerChangeToReservation(event, addressText, clientObj);
    }

    if (addressText.length < 3) {
      processingOrders.delete(customerLineId);
      return replyText(clientObj, event.replyToken, "請輸入完整地址");
    }

    const order = await createOrder(addressText, customerLineId, source);
    totalOrders++;

    const preference = await getCustomerPreference(customerLineId);
    const paymentText =
      preference && preference.payment_method ? ` ${preference.payment_method}` : "";

    const fee = cancelFees.get(customerLineId) || 0;
    const feeText = fee > 0 ? ` 代收取消費${fee}` : "";

    processingOrders.delete(customerLineId);

    await replyText(clientObj, event.replyToken, "立即為您派車");

    await enqueueMessage({
      toId: DRIVER_GROUP_ID,
      sourceName: DRIVER_GROUP_SOURCE,
      priority: PRIORITY_NEW_ORDER,
      message: {
        type: "text",
        text: `${order.order_code} ${order.address}${paymentText}${feeText}`
      }
    });
  } catch (err) {
    processingOrders.delete(customerLineId);
    console.error("handleCustomerOrder error:", err);
    return replyText(clientObj, event.replyToken, "系統忙碌中");
  }
}

async function handleCustomerChangeToReservation(event, text, clientObj) {
  const parts = text.split(/\s+/);

  if (parts.length < 2) {
    return replyText(clientObj, event.replyToken, "格式錯誤\n例如：改預約 18:30");
  }

  const reservationTime = parts[1];
  const order = await getLatestCustomerOrder(event.source.userId);

  if (!order) {
    return replyText(clientObj, event.replyToken, "找不到您目前的訂單");
  }

  const orderSource = order.source_name || "A";
  const preference = await getCustomerPreference(event.source.userId);
  const paymentText =
    preference && preference.payment_method ? ` ${preference.payment_method}` : "";

  if (order.status !== "assigned" || !order.assigned_driver_line_id) {
    await enqueueMessage({
      toId: DRIVER_GROUP_ID,
      sourceName: DRIVER_GROUP_SOURCE,
      priority: PRIORITY_NEW_ORDER,
      message: {
        type: "text",
        text: `${order.order_code} ${reservationTime} ${order.address}${paymentText}`
      }
    });

    return replyText(
      clientObj,
      event.replyToken,
      `已改成預約單\n預約時間:${reservationTime}`
    );
  }

  pendingReservationChanges.set(order.order_code, {
    order,
    reservationTime,
    address: order.address,
    paymentText,
    customerLineId: order.customer_line_id,
    driverLineId: order.assigned_driver_line_id,
    source: orderSource
  });

  await pushAskDriverReservationChange(order, reservationTime, paymentText);

  return replyText(clientObj, event.replyToken, "已詢問司機是否可更改，請稍等");
}

async function handleReservationDriverReply(event, text, clientObj) {
  const cleanText = text.trim();

  if (cleanText !== "可" && cleanText !== "不同意") {
    return false;
  }

  for (const [orderCode, pending] of pendingReservationChanges.entries()) {
    if (pending.driverLineId !== event.source.userId) continue;

    const orderSource = pending.source || "A";

    if (cleanText === "可") {
      pendingReservationChanges.delete(orderCode);

      await pushCustomerReservationChanged(
        pending.customerLineId,
        pending.reservationTime,
        orderSource
      );

      await replyMention(clientObj, event.replyToken, event.source.userId, "可");
      return true;
    }

    if (cleanText === "不同意") {
      pendingReservationChanges.delete(orderCode);

      await replyMention(clientObj, event.replyToken, event.source.userId, "X");

      await resetOrderForReDispatch(pending.order.order_id);

      await enqueueMessage({
        toId: DRIVER_GROUP_ID,
        sourceName: DRIVER_GROUP_SOURCE,
        priority: PRIORITY_NEW_ORDER,
        message: {
          type: "text",
          text: `${pending.order.order_code} ${pending.reservationTime} ${pending.address}${pending.paymentText || ""}`
        }
      });

      return true;
    }
  }

  return false;
}

async function handleDriverReport(event, text, clientObj, parsedStrict = null) {
  const parsed = parsedStrict || parseStrictDriverMessage(text);

  if (!parsed) return replyMention(clientObj, event.replyToken, event.source.userId, "X");

  const orderCode = parsed.orderCode;
  const address = normalizeReportedAddress(parsed.address);
  const plate = parsed.plate;
  const minutes = Number(parsed.minutes);

  const order = await getOrderByCodeAndAddress(orderCode, address);

  if (!order) return replyMention(clientObj, event.replyToken, event.source.userId, "X");

  const orderSource = order.source_name || "A";

  if (order.status === "canceled") {
    return replyMention(clientObj, event.replyToken, event.source.userId, "X");
  }

  if (parsed.type === "arrived") {
    await upsertDriverCurrentOrder({
      driverLineId: event.source.userId,
      orderId: order.order_id,
      orderCode,
      address,
      plate,
      status: "arrived"
    });

    await pushCustomerArrived(order.customer_line_id, plate, orderSource);
    return;
  }

  if (parsed.type === "customer_on") {
    await upsertDriverCurrentOrder({
      driverLineId: event.source.userId,
      orderId: order.order_id,
      orderCode,
      address,
      plate,
      status: "customer_on"
    });

    return;
  }

  if (order.status === "assigned") {
    const oldArrival = getArrivalTimeMs(order.assigned_at, order.assigned_minutes);
    const newArrival = Date.now() + minutes * 60 * 1000;
    const diffMinutes = (oldArrival - newArrival) / 1000 / 60;

    if (diffMinutes < OVERRIDE_DIFF_MINUTES) {
      return replyMention(clientObj, event.replyToken, event.source.userId, "X");
    }

    const oldDriverLineId = order.assigned_driver_line_id;

    const updatedOrder = await overrideDriver({
      order,
      driverLineId: event.source.userId,
      plate,
      minutes
    });

    await rememberDriverOrder({
      driverLineId: event.source.userId,
      order: updatedOrder,
      orderCode,
      address,
      plate
    });

    await queueTwoMentions(oldDriverLineId, event.source.userId);

    await pushCustomerDispatch(
      updatedOrder.customer_line_id,
      plate,
      minutes,
      orderSource
    );

    totalAssigned++;
    return;
  }

  const firstReport = await getFirstDriverReport(order.order_id);

  if (firstReport) {
    const firstArrival = getArrivalTimeMs(firstReport.created_at, firstReport.minutes);
    const newArrival = Date.now() + minutes * 60 * 1000;
    const diffMinutes = (firstArrival - newArrival) / 1000 / 60;

    if (diffMinutes < COMPETE_DIFF_MINUTES) {
      return replyMention(clientObj, event.replyToken, event.source.userId, "X");
    }
  }

  try {
    await addDriverReport({
      orderId: order.order_id,
      orderCode,
      address,
      driverLineId: event.source.userId,
      plate,
      minutes
    });
  } catch (err) {
    if (err.code === "23505") {
      return replyMention(clientObj, event.replyToken, event.source.userId, "X");
    }

    throw err;
  }

  if (!order.decision_started && !decidingOrders.has(order.order_id)) {
    await assignWinnerDriver(order.order_id);

    decidingOrders.add(order.order_id);

    setTimeout(async () => {
      if (!BOT_ENABLED) {
        decidingOrders.delete(order.order_id);
        return;
      }

      try {
        const result = await decideWinner(order.order_id);
        if (!result) return;

        const { order: assignedOrder, winner } = result;
        const winnerSource = assignedOrder.source_name || orderSource || "A";

        await rememberDriverOrder({
          driverLineId: winner.driver_line_id,
          order: assignedOrder,
          orderCode: winner.order_code,
          address: winner.address,
          plate: winner.plate
        });

        await queueGroupMention(
          winner.driver_line_id,
          "噴",
          assignedOrder.order_id,
          PRIORITY_COUNTDOWN_SPRAY
        );

        await pushCustomerDispatch(
          assignedOrder.customer_line_id,
          winner.plate,
          winner.minutes,
          winnerSource
        );

        totalAssigned++;
      } catch (err) {
        console.error("decideWinner error:", err);
      } finally {
        decidingOrders.delete(order.order_id);
      }
    }, 8000);
  }
}

async function refreshOpenOrders() {
  if (!BOT_ENABLED) return;
  if (!REFRESH_ENABLED) return;

  try {
    const orders = await getOpenOrdersForRefresh();

    if (!orders || orders.length === 0) return;

    const refreshTargets = orders.slice(0, REFRESH_BATCH_SIZE);

    await queueRefreshText(
      DRIVER_GROUP_ID,
      "🪳---🪳 我是分隔線 🪳---🪳",
      DRIVER_GROUP_SOURCE
    );

    for (const order of refreshTargets) {
      if (refreshingOrders.has(order.order_id)) continue;

      refreshingOrders.add(order.order_id);

      const preference = await getCustomerPreference(order.customer_line_id);

      const paymentText =
        preference && preference.payment_method ? ` ${preference.payment_method}` : "";

      await queueRefreshText(
        DRIVER_GROUP_ID,
        `${order.order_code} ${order.address}${paymentText}`,
        DRIVER_GROUP_SOURCE
      );

      await markOrderRefreshed(order.order_id);

      refreshingOrders.delete(order.order_id);

      await delay(1000);
    }
  } catch (err) {
    refreshingOrders.clear();
    console.error("refreshOpenOrders error:", err);
  }
}

setInterval(processMessageJobs, MESSAGE_WORKER_INTERVAL_MS);
setInterval(refreshOpenOrders, REFRESH_INTERVAL_MS);

setInterval(() => {
  console.log(
    `429:${total429}
GoogleAPI:${GOOGLE_API_ENABLED ? "ON" : "OFF"}`
  );
}, 5 * 60 * 1000);

app.get("/", (req, res) => {
  res.send(`BOT=${BOT_ENABLED} REFRESH=${REFRESH_ENABLED} GOOGLE=${GOOGLE_API_ENABLED}`);
});

async function loadBotSettings() {
  const botEnabled = await getBotSetting("bot_enabled");
  const refreshEnabled = await getBotSetting("refresh_enabled");

  BOT_ENABLED = botEnabled !== "false";
  REFRESH_ENABLED = refreshEnabled !== "false";
}

const port = process.env.PORT || 3000;

loadBotSettings().then(() => {
  app.listen(port, () => {
    console.log("BOT RUNNING:", port);
    console.log("官方A: ON");
    console.log("官方B:", hasLineB ? "ON" : "OFF");
    console.log("Supabase message_jobs: ON");
    console.log("Priority: countdown > order > refresh");
    console.log("Google API:", GOOGLE_API_ENABLED ? "ON" : "OFF");
  });
});