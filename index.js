require("dotenv").config();

const express = require("express");
const line = require("@line/bot-sdk");

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
  getDriverCurrentOrder,
  upsertDriverCurrentOrder,
  getBotSetting,
  setBotSetting
} = require("./services/orderService");

const { parseDriverMessage } = require("./utils/parser");
const { getDrivingMinutes } = require("./services/googleDistanceService");

const app = express();

const configA = {
  channelAccessToken: process.env.LINE_CHANNEL_ACCESS_TOKEN,
  channelSecret: process.env.LINE_CHANNEL_SECRET
};

const clientA = new line.Client(configA);

let configB = null;
let clientB = null;

const hasLineB =
  !!process.env.LINE_B_CHANNEL_ACCESS_TOKEN &&
  !!process.env.LINE_B_CHANNEL_SECRET;

if (hasLineB) {
  configB = {
    channelAccessToken: process.env.LINE_B_CHANNEL_ACCESS_TOKEN,
    channelSecret: process.env.LINE_B_CHANNEL_SECRET
  };

  clientB = new line.Client(configB);
  console.log("官方B 已啟用");
} else {
  console.log("官方B 尚未啟用");
}

const DRIVER_GROUP_ID = process.env.DRIVER_GROUP_ID;

let BOT_ENABLED = true;
let REFRESH_ENABLED = true;

const COMPETE_DIFF_MINUTES = 3;
const OVERRIDE_DIFF_MINUTES = 7;
const INSTANT_WIN_MINUTES = 5;
const GOOGLE_TOLERANCE_MINUTES = 3;

const REFRESH_INTERVAL_MS = 45 * 1000;
const REFRESH_BATCH_SIZE = 3;

const PUSH_GAP_MS = 4000;
const MIN_PUSH_GAP_MS = 3000;
const MAX_PUSH_GAP_MS = 15000;
const MAX_QUEUE_SIZE = 300;
const MAX_RETRY = 5;

let currentPushGapMs = PUSH_GAP_MS;
let isPushSending = false;

let circuitBreaker = false;
let tooMany429Count = 0;
let pauseRefreshUntil = 0;

const criticalQueue = [];
const normalQueue = [];
const refreshQueue = [];

const processingOrders = new Set();
const refreshingOrders = new Set();
const decidingOrders = new Set();
const pendingReservationChanges = new Map();

const customerCooldown = new Map();
const driverCooldown = new Map();

const blacklistCustomers = new Set();
const cancelFees = new Map();

let totalOrders = 0;
let total429 = 0;
let totalCanceled = 0;
let totalAssigned = 0;

const strictDriverRegex =
  /^#?[A-Z]\d\/\s+[\s\S]+\s+[\u4e00-\u9fa5A-Za-z0-9\-]+\s+\d+\s*$/;

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function getClientBySource(source) {
  if (source === "B" && clientB) return clientB;
  return clientA;
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

function addQueue(queue, job) {
  const totalSize =
    criticalQueue.length + normalQueue.length + refreshQueue.length;

  if (totalSize >= MAX_QUEUE_SIZE) {
    console.error("QUEUE FULL");
    return;
  }

  queue.push(job);
  processPushQueue();
}

function queueCriticalMessage(to, message, source = "A") {
  addQueue(criticalQueue, {
    to,
    retry: 0,
    source,
    priority: "critical",
    message
  });
}

function queueCriticalText(to, text, source = "A") {
  queueCriticalMessage(
    to,
    {
      type: "text",
      text
    },
    source
  );
}

function queueNormalText(to, text, source = "A") {
  addQueue(normalQueue, {
    to,
    retry: 0,
    source,
    priority: "normal",
    message: {
      type: "text",
      text
    }
  });
}

function queueRefreshText(to, text, source = "A") {
  if (!REFRESH_ENABLED) return;
  if (circuitBreaker) return;
  if (Date.now() < pauseRefreshUntil) return;

  addQueue(refreshQueue, {
    to,
    retry: 0,
    source,
    priority: "refresh",
    message: {
      type: "text",
      text
    }
  });
}

async function processPushQueue() {
  if (isPushSending) return;

  isPushSending = true;

  while (
    criticalQueue.length > 0 ||
    normalQueue.length > 0 ||
    refreshQueue.length > 0
  ) {
    let job;

    if (criticalQueue.length > 0) {
      job = criticalQueue.shift();
    } else if (normalQueue.length > 0) {
      job = normalQueue.shift();
    } else {
      job = refreshQueue.shift();
    }

    try {
      const targetClient = getClientBySource(job.source);

      await targetClient.pushMessage(job.to, job.message);

      currentPushGapMs = Math.max(
        MIN_PUSH_GAP_MS,
        currentPushGapMs - 300
      );

// ====================================
// 新單插隊機制
// ====================================

let waitMs = currentPushGapMs;

while (waitMs > 0) {

  // 每500ms檢查一次
  await delay(500);

  waitMs -= 500;

  // 如果現在正在刷單
  // 但有新 critical
  // 立刻中斷等待
  if (
    job.priority === "refresh" &&
    criticalQueue.length > 0
  ) {

    break;
  }
}
    } catch (err) {
      const status = getErrorStatus(err);
      const data = getErrorData(err);

      console.error("PUSH ERROR:", data);

      if (
        data &&
        typeof data === "object" &&
        data.message === "You have reached your monthly limit."
      ) {
        criticalQueue.length = 0;
        normalQueue.length = 0;
        refreshQueue.length = 0;
        break;
      }

      if (status === 429 && job.retry < MAX_RETRY) {
        total429++;
        job.retry += 1;
        tooMany429Count++;

        REFRESH_ENABLED = false;
        pauseRefreshUntil = Date.now() + 5 * 60 * 1000;

        if (tooMany429Count >= 5) {
          circuitBreaker = true;

          setTimeout(() => {
            circuitBreaker = false;
            tooMany429Count = 0;
            REFRESH_ENABLED = true;
          }, 10 * 60 * 1000);
        }

        currentPushGapMs = Math.min(
          MAX_PUSH_GAP_MS,
          currentPushGapMs + 1500
        );

        await delay(currentPushGapMs * job.retry);

        if (job.priority === "critical") {
          criticalQueue.unshift(job);
        } else if (job.priority === "normal") {
          normalQueue.unshift(job);
        } else {
          refreshQueue.unshift(job);
        }

        continue;
      }

      await delay(currentPushGapMs);
    }
  }

  isPushSending = false;
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

function queueGroupMention(userId, text, source = "A") {
  queueCriticalMessage(
    DRIVER_GROUP_ID,
    {
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
    },
    source
  );
}

function queueTwoMentions(oldUserId, newUserId, source = "A") {
  queueCriticalMessage(
    DRIVER_GROUP_ID,
    {
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
    },
    source
  );
}

function pushCustomerDispatch(customerLineId, plate, minutes, source = "A") {
  queueCriticalText(
    customerLineId,
    `司機已出發\n車牌:${plate}\n約${minutes}分鐘`,
    source
  );
}

function pushCustomerArrived(customerLineId, plate, source = "A") {
  queueCriticalText(customerLineId, `車輛已抵達\n車牌:${plate}`, source);
}

function pushCustomerReservationChanged(
  customerLineId,
  reservationTime,
  source = "A"
) {
  queueCriticalText(
    customerLineId,
    `已為您更改為預約單\n預約時間:${reservationTime}`,
    source
  );
}

function pushAskDriverReservationChange(
  order,
  reservationTime,
  paymentText = "",
  source = "A"
) {
  queueCriticalMessage(
    DRIVER_GROUP_ID,
    {
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
    },
    source
  );
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
  const minutesText = parts[parts.length - 1];
  const plate = parts[parts.length - 2];
  const address = parts.slice(1, parts.length - 2).join(" ");

  const minutes = Number(minutesText.replace("分鐘", "").replace("分", ""));

  if (!orderCode.startsWith("#")) return null;
  if (!Number.isFinite(minutes)) return null;
  if (!address || !plate) return null;

  return {
    type: "report",
    orderCode,
    address,
    plate,
    minutes
  };
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

async function checkDriverCurrentOrderTime({
  clientObj,
  driverLineId,
  replyToken,
  currentOrder,
  newAddress,
  reportMinutes
}) {
  if (!currentOrder || !currentOrder.address) return true;

  const googleMinutes = await getDrivingMinutes(
    currentOrder.address,
    newAddress
  );

  if (googleMinutes === null) return true;

  if (Number(reportMinutes) < googleMinutes - GOOGLE_TOLERANCE_MINUTES) {
    await replyMention(clientObj, replyToken, driverLineId, "X");
    return false;
  }

  return true;
}

async function handleBotControl(event, text, clientObj) {
  if (event.source.type !== "group") return false;

  if (text === "停止機器人運作" || text === "停止") {
    BOT_ENABLED = false;
    await setBotSetting("bot_enabled", "false");

    criticalQueue.length = 0;
    normalQueue.length = 0;
    refreshQueue.length = 0;

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

    refreshQueue.length = 0;

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
    await replyText(
      clientObj,
      event.replyToken,
      `BOT:${BOT_ENABLED}
REFRESH:${REFRESH_ENABLED}
429:${total429}
總訂單:${totalOrders}
已取消:${totalCanceled}
已派送:${totalAssigned}
Critical:${criticalQueue.length}
Normal:${normalQueue.length}
Refresh:${refreshQueue.length}`
    );

    return true;
  }

  return false;
}

app.post("/webhook", line.middleware(configA), async (req, res) => {
  res.status(200).end();

  const events = req.body.events || [];

  for (const event of events) {
    handleEvent(event, clientA, "A").catch(err => {
      console.error("A error:", err);
    });
  }
});

if (hasLineB) {
  app.post("/webhook-b", line.middleware(configB), async (req, res) => {
    res.status(200).end();

    const events = req.body.events || [];

    for (const event of events) {
      handleEvent(event, clientB, "B").catch(err => {
        console.error("B error:", err);
      });
    }
  });
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
      const reservationReply = await handleReservationDriverReply(
        event,
        text,
        clientObj,
        source
      );

      if (reservationReply) return;
    }

    if (!checkDriverCooldown(event.source.userId)) return;

    if (!text.startsWith("#")) return;

    if (!strictDriverRegex.test(text)) {
      console.log("非法格式:", text);
      return;
    }

    return handleDriverReport(event, text, clientObj, source);
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

      const orderSource = canceledOrder.source_name || source || "A";

      if (canceledOrder.assigned_driver_line_id) {
        queueGroupMention(
          canceledOrder.assigned_driver_line_id,
          "取",
          orderSource
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

      const cleanedAddress = removePaymentKeyword(
        addressText,
        paymentDetected.keyword
      );

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
      return handleCustomerChangeToReservation(
        event,
        addressText,
        clientObj,
        source
      );
    }

    if (addressText.length < 3) {
      processingOrders.delete(customerLineId);
      return replyText(clientObj, event.replyToken, "請輸入完整地址");
    }

    const order = await createOrder(addressText, customerLineId, source);
    totalOrders++;

    const preference = await getCustomerPreference(customerLineId);

    const paymentText =
      preference && preference.payment_method
        ? ` ${preference.payment_method}`
        : "";

    const fee = cancelFees.get(customerLineId) || 0;
    const feeText = fee > 0 ? ` 代收取消費${fee}` : "";

    processingOrders.delete(customerLineId);

    await replyText(clientObj, event.replyToken, "立即為您派車");

    queueCriticalText(
      DRIVER_GROUP_ID,
      `${order.order_code} ${order.address}${paymentText}${feeText}`,
      source
    );
  } catch (err) {
    processingOrders.delete(customerLineId);
    console.error("handleCustomerOrder error:", err);
    return replyText(clientObj, event.replyToken, "系統忙碌中");
  }
}

async function handleCustomerChangeToReservation(
  event,
  text,
  clientObj,
  source
) {
  const parts = text.split(/\s+/);

  if (parts.length < 2) {
    return replyText(clientObj, event.replyToken, "格式錯誤\n例如：改預約 18:30");
  }

  const reservationTime = parts[1];
  const order = await getLatestCustomerOrder(event.source.userId);

  if (!order) {
    return replyText(clientObj, event.replyToken, "找不到您目前的訂單");
  }

  const orderSource = order.source_name || source || "A";
  const preference = await getCustomerPreference(event.source.userId);

  const paymentText =
    preference && preference.payment_method
      ? ` ${preference.payment_method}`
      : "";

  if (order.status !== "assigned" || !order.assigned_driver_line_id) {
    queueCriticalText(
      DRIVER_GROUP_ID,
      `${order.order_code} ${reservationTime} ${order.address}${paymentText}`,
      orderSource
    );

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

  pushAskDriverReservationChange(order, reservationTime, paymentText, orderSource);

  return replyText(clientObj, event.replyToken, "已詢問司機是否可更改，請稍等");
}

async function handleReservationDriverReply(event, text, clientObj, source) {
  const cleanText = text.trim();

  if (cleanText !== "可" && cleanText !== "不同意") {
    return false;
  }

  for (const [orderCode, pending] of pendingReservationChanges.entries()) {
    if (pending.driverLineId !== event.source.userId) continue;

    const orderSource = pending.source || source || "A";

    if (cleanText === "可") {
      pendingReservationChanges.delete(orderCode);

      pushCustomerReservationChanged(
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

      queueCriticalText(
        DRIVER_GROUP_ID,
        `${pending.order.order_code} ${pending.reservationTime} ${pending.address}${pending.paymentText || ""}`,
        orderSource
      );

      return true;
    }
  }

  return false;
}

async function handleDriverReport(event, text, clientObj, source) {
  const parsed = parseDriverMessage(text) || parseStrictDriverMessage(text);
  if (!parsed) return;

  const orderCode = parsed.orderCode;
  const address = normalizeReportedAddress(parsed.address);
  const plate = parsed.plate;
  const minutes = Number(parsed.minutes);

  const order = await getOrderByCodeAndAddress(orderCode, address);
  if (!order) return;

  const orderSource = order.source_name || source || "A";

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

    pushCustomerArrived(order.customer_line_id, plate, orderSource);
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

  const currentDriverOrder = await getDriverCurrentOrder(event.source.userId);

  if (currentDriverOrder && currentDriverOrder.order_id !== order.order_id) {
    const ok = await checkDriverCurrentOrderTime({
      clientObj,
      driverLineId: event.source.userId,
      replyToken: event.replyToken,
      currentOrder: currentDriverOrder,
      newAddress: address,
      reportMinutes: minutes
    });

    if (!ok) return;
  }

  if (order.status === "assigned") {
    const oldArrival = getArrivalTimeMs(
      order.assigned_at,
      order.assigned_minutes
    );

    const newArrival = Date.now() + Number(minutes) * 60 * 1000;
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

    queueTwoMentions(oldDriverLineId, event.source.userId, orderSource);

    pushCustomerDispatch(
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
    const firstArrival = getArrivalTimeMs(
      firstReport.created_at,
      firstReport.minutes
    );

    const newArrival = Date.now() + Number(minutes) * 60 * 1000;
    const diffMinutes = (firstArrival - newArrival) / 1000 / 60;

    if (diffMinutes < COMPETE_DIFF_MINUTES) {
      return replyMention(clientObj, event.replyToken, event.source.userId, "X");
    }
  }

  await addDriverReport({
    orderId: order.order_id,
    orderCode,
    address,
    driverLineId: event.source.userId,
    plate,
    minutes
  });

  if (Number(minutes) <= INSTANT_WIN_MINUTES) {
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

    await replyMention(clientObj, event.replyToken, event.source.userId, "噴");

    pushCustomerDispatch(
      updatedOrder.customer_line_id,
      plate,
      minutes,
      orderSource
    );

    totalAssigned++;
    return;
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

        queueGroupMention(winner.driver_line_id, "噴", winnerSource);

        pushCustomerDispatch(
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
    }, 10000);
  }
}

async function refreshOpenOrders() {
  if (!BOT_ENABLED) return;
  if (!REFRESH_ENABLED) return;
  if (circuitBreaker) return;
  if (Date.now() < pauseRefreshUntil) return;

  try {
    const orders = await getOpenOrdersForRefresh();

    if (!orders || orders.length === 0) return;

    const refreshTargets = orders.slice(0, REFRESH_BATCH_SIZE);

    queueRefreshText(
      DRIVER_GROUP_ID,
      "🪳---🪳 我是分隔線 🪳---🪳",
      "A"
    );

    for (const order of refreshTargets) {
      if (refreshingOrders.has(order.order_id)) continue;

      refreshingOrders.add(order.order_id);

      const preference = await getCustomerPreference(order.customer_line_id);

      const paymentText =
        preference && preference.payment_method
          ? ` ${preference.payment_method}`
          : "";

      const orderSource = order.source_name || "A";

      queueRefreshText(
        DRIVER_GROUP_ID,
        `${order.order_code} ${order.address}${paymentText}`,
        orderSource
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

setInterval(() => {
  if (!REFRESH_ENABLED && !circuitBreaker && Date.now() > pauseRefreshUntil) {
    REFRESH_ENABLED = true;
  }
}, 60 * 1000);

setInterval(() => {
  console.log(
    `Critical:${criticalQueue.length}
Normal:${normalQueue.length}
Refresh:${refreshQueue.length}
429:${total429}`
  );
}, 5 * 60 * 1000);

setInterval(refreshOpenOrders, REFRESH_INTERVAL_MS);

app.get("/", (req, res) => {
  res.send(`BOT=${BOT_ENABLED} REFRESH=${REFRESH_ENABLED}`);
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
    console.log("LINE_B_SECRET exists:", !!process.env.LINE_B_CHANNEL_SECRET);
    console.log("LINE_B_TOKEN exists:", !!process.env.LINE_B_CHANNEL_ACCESS_TOKEN);
  });
});