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
  assignWinnerDriver,
  overrideDriver,
  getFirstDriverReport,
  getDriverCurrentOrder,
  upsertDriverCurrentOrder
} = require("./services/orderService");

const { replyText } = require("./services/lineService");
const { parseDriverMessage } = require("./utils/parser");
const { getDrivingMinutes } = require("./services/googleDistanceService");

const app = express();

const config = {
  channelAccessToken: process.env.LINE_CHANNEL_ACCESS_TOKEN,
  channelSecret: process.env.LINE_CHANNEL_SECRET
};

const client = new line.Client(config);
const DRIVER_GROUP_ID = process.env.DRIVER_GROUP_ID;

const COMPETE_DIFF_MINUTES = 3;
const OVERRIDE_DIFF_MINUTES = 7;
const INSTANT_WIN_MINUTES = 5;
const GOOGLE_TOLERANCE_MINUTES = 3;

const pendingReservationChanges = new Map();

const pushQueue = [];
let isPushSending = false;

const PUSH_GAP_MS = 2500;
const MIN_PUSH_GAP_MS = 1800;
const MAX_PUSH_GAP_MS = 8000;
const MERGE_WINDOW_MS = 1500;
const MAX_RETRY = 5;

let currentPushGapMs = PUSH_GAP_MS;

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function getArrivalTimeMs(reportTime, minutes) {
  return new Date(reportTime).getTime() + Number(minutes) * 60 * 1000;
}

function getErrorStatus(err) {
  return err?.statusCode || err?.originalError?.response?.status;
}

function getErrorData(err) {
  return err?.originalError?.response?.data || err.message;
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
        return {
          keyword,
          value: rule.value
        };
      }
    }
  }

  return null;
}

function removePaymentKeyword(text, keyword) {
  return text.replace(keyword, "").replace(/\s+/g, " ").trim();
}

function queuePushMessage(to, message, options = {}) {
  if (!to || !message) return;

  const now = Date.now();

  if (
    options.merge !== false &&
    message.type === "text" &&
    pushQueue.length > 0
  ) {
    const lastJob = pushQueue[pushQueue.length - 1];

    if (
      lastJob &&
      lastJob.to === to &&
      lastJob.message.type === "text" &&
      now - lastJob.createdAt <= MERGE_WINDOW_MS
    ) {
      lastJob.message.text += "\n" + message.text;
      return;
    }
  }

  pushQueue.push({
    to,
    message,
    retry: 0,
    createdAt: now
  });

  processPushQueue();
}

function queuePushText(to, text, options = {}) {
  queuePushMessage(
    to,
    {
      type: "text",
      text
    },
    options
  );
}

async function processPushQueue() {
  if (isPushSending) return;

  isPushSending = true;

  while (pushQueue.length > 0) {
    const job = pushQueue.shift();

    try {
      await client.pushMessage(job.to, job.message);

      console.log("PUSH SEND:", job.message.text || job.message.type);

      currentPushGapMs = Math.max(
        MIN_PUSH_GAP_MS,
        currentPushGapMs - 300
      );

      await delay(currentPushGapMs);
    } catch (err) {
      const status = getErrorStatus(err);
      const data = getErrorData(err);

      console.error("PUSH ERROR:", data);

      if (
        data &&
        typeof data === "object" &&
        data.message === "You have reached your monthly limit."
      ) {
        console.error("LINE 月額度已用完，停止 pushMessage");
        pushQueue.length = 0;
        break;
      }

      if (status === 429 && job.retry < MAX_RETRY) {
        job.retry += 1;

        currentPushGapMs = Math.min(
          MAX_PUSH_GAP_MS,
          currentPushGapMs + 1500
        );

        const retryDelay = currentPushGapMs * job.retry;

        console.log(`429 限流，${retryDelay}ms 後重試`);

        await delay(retryDelay);
        pushQueue.unshift(job);
        continue;
      }

      await delay(currentPushGapMs);
    }
  }

  isPushSending = false;
}

async function replyGroupMention(replyToken, userId, text) {
  try {
    return client.replyMessage(replyToken, {
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
  } catch (err) {
    console.error("replyGroupMention error:", getErrorData(err));
  }
}

async function replyTwoGroupMentions(replyToken, oldUserId, newUserId) {
  try {
    return client.replyMessage(replyToken, {
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
    });
  } catch (err) {
    console.error("replyTwoGroupMentions error:", getErrorData(err));
  }
}

function pushGroupWinnerMention(driverLineId) {
  queuePushMessage(
    DRIVER_GROUP_ID,
    {
      type: "textV2",
      text: "{driver} 噴",
      substitution: {
        driver: {
          type: "mention",
          mentionee: {
            type: "user",
            userId: driverLineId
          }
        }
      }
    },
    { merge: false }
  );
}

function pushCustomerDispatch(customerLineId, plate, minutes) {
  queuePushText(
    customerLineId,
    `司機已出發\n車牌：${plate}\n約 ${minutes} 分鐘到`,
    { merge: false }
  );
}

function pushCustomerArrived(customerLineId, plate) {
  queuePushText(
    customerLineId,
    `車輛已抵達\n車牌：${plate}`,
    { merge: false }
  );
}

function pushCustomerReservationChanged(customerLineId, reservationTime) {
  queuePushText(
    customerLineId,
    `已為您更改為預約單\n預約時間：${reservationTime}`,
    { merge: false }
  );
}

function pushAskDriverReservationChange(order, reservationTime, paymentText = "") {
  queuePushMessage(
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
    { merge: false }
  );
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
    await replyGroupMention(replyToken, driverLineId, "X");
    return false;
  }

  return true;
}

app.get("/", (req, res) => {
  res.send("Taxi Dispatch Bot Running - Safe Queue Mode");
});

app.post("/webhook", line.middleware(config), async (req, res) => {
  res.status(200).end();

  const events = req.body.events || [];

  for (const event of events) {
    handleEvent(event).catch(err => {
      console.error("handleEvent async error:", err);
    });
  }
});

async function handleEvent(event) {
  console.log("SOURCE:", event.source);

  if (event.type !== "message") return;
  if (event.message.type !== "text") return;

  const text = event.message.text.trim();

  if (event.source.type === "user") {
    return handleCustomerOrder(event, text);
  }

  if (event.source.type === "group") {
    return handleDriverReport(event, text);
  }
}

async function handleCustomerOrder(event, addressText) {
  const customerLineId = event.source.userId;

  if (addressText === "取消付款設定") {
    await upsertCustomerPreference(customerLineId, "");

    return replyText(
      client,
      event.replyToken,
      "已取消您的固定付款方式"
    );
  }

  const paymentDetected = detectPaymentMethod(addressText);

  if (paymentDetected) {
    await upsertCustomerPreference(customerLineId, paymentDetected.value);

    const cleanedAddress = removePaymentKeyword(
      addressText,
      paymentDetected.keyword
    );

    if (cleanedAddress.length < 3) {
      return replyText(
        client,
        event.replyToken,
        `已記住您的付款方式：${paymentDetected.value}`
      );
    }

    addressText = cleanedAddress;
  }

  if (addressText.startsWith("改預約")) {
    return handleCustomerChangeToReservation(event, addressText);
  }

  if (addressText.length < 3) {
    return replyText(client, event.replyToken, "請輸入完整地址");
  }

  const order = await createOrder(addressText, customerLineId);

  const preference = await getCustomerPreference(customerLineId);

  const paymentText =
    preference && preference.payment_method
      ? ` ${preference.payment_method}`
      : "";

  await replyText(
    client,
    event.replyToken,
    "立即為您派車，請稍等"
  );

  queuePushText(
    DRIVER_GROUP_ID,
    `${order.order_code} ${order.address}${paymentText}`
  );
}

async function handleCustomerChangeToReservation(event, text) {
  const parts = text.split(/\s+/);

  if (parts.length < 2) {
    return replyText(
      client,
      event.replyToken,
      "格式錯誤\n例如：改預約 18:30"
    );
  }

  const reservationTime = parts[1];

  const order = await getLatestCustomerOrder(event.source.userId);

  if (!order) {
    return replyText(client, event.replyToken, "找不到您目前的訂單");
  }

  const preference = await getCustomerPreference(event.source.userId);

  const paymentText =
    preference && preference.payment_method
      ? ` ${preference.payment_method}`
      : "";

  if (order.status !== "assigned" || !order.assigned_driver_line_id) {
    queuePushText(
      DRIVER_GROUP_ID,
      `${order.order_code} ${reservationTime} ${order.address}${paymentText}`,
      { merge: false }
    );

    return replyText(
      client,
      event.replyToken,
      `已改成預約單\n預約時間：${reservationTime}`
    );
  }

  pendingReservationChanges.set(order.order_code, {
    order,
    reservationTime,
    address: order.address,
    paymentText,
    customerLineId: order.customer_line_id,
    driverLineId: order.assigned_driver_line_id
  });

  pushAskDriverReservationChange(order, reservationTime, paymentText);

  return replyText(
    client,
    event.replyToken,
    "已詢問司機是否可更改，請稍等"
  );
}

async function handleDriverReport(event, text) {
  const reservationReply = await handleReservationDriverReply(event, text);
  if (reservationReply) return;

  const parsed = parseDriverMessage(text);

  if (!parsed) {
    return replyGroupMention(event.replyToken, event.source.userId, "X");
  }

  const { orderCode, address, plate, minutes } = parsed;

  const order = await getOrderByCodeAndAddress(orderCode, address);
  if (!order) return;

  if (parsed.type === "arrived") {
    await upsertDriverCurrentOrder({
      driverLineId: event.source.userId,
      orderId: order.order_id,
      orderCode,
      address,
      plate,
      status: "arrived"
    });

    pushCustomerArrived(order.customer_line_id, plate);
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

  if (parsed.type !== "report") return;

  const currentDriverOrder = await getDriverCurrentOrder(event.source.userId);

  if (
    currentDriverOrder &&
    currentDriverOrder.order_id !== order.order_id
  ) {
    const ok = await checkDriverCurrentOrderTime({
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
      return replyGroupMention(event.replyToken, event.source.userId, "X");
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

    await replyTwoGroupMentions(
      event.replyToken,
      oldDriverLineId,
      event.source.userId
    );

    pushCustomerDispatch(
      updatedOrder.customer_line_id,
      plate,
      minutes
    );

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
      return replyGroupMention(event.replyToken, event.source.userId, "X");
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

    await replyGroupMention(event.replyToken, event.source.userId, "噴");

    pushCustomerDispatch(
      updatedOrder.customer_line_id,
      plate,
      minutes
    );

    return;
  }

  if (!order.decision_started) {
    await assignWinnerDriver(order.order_id);

    setTimeout(async () => {
      try {
        const result = await decideWinner(order.order_id);
        if (!result) return;

        const { order: assignedOrder, winner } = result;

        await rememberDriverOrder({
          driverLineId: winner.driver_line_id,
          order: assignedOrder,
          orderCode: winner.order_code,
          address: winner.address,
          plate: winner.plate
        });

        pushGroupWinnerMention(winner.driver_line_id);

        pushCustomerDispatch(
          assignedOrder.customer_line_id,
          winner.plate,
          winner.minutes
        );
      } catch (err) {
        console.error("decideWinner error:", err);
      }
    }, 10000);
  }
}

async function handleReservationDriverReply(event, text) {
  const cleanText = text.trim();

  if (cleanText !== "可" && cleanText !== "不同意") {
    return false;
  }

  for (const [orderCode, pending] of pendingReservationChanges.entries()) {
    if (pending.driverLineId !== event.source.userId) {
      continue;
    }

    if (cleanText === "可") {
      pendingReservationChanges.delete(orderCode);

      pushCustomerReservationChanged(
        pending.customerLineId,
        pending.reservationTime
      );

      await replyGroupMention(
        event.replyToken,
        event.source.userId,
        "可"
      );

      return true;
    }

    if (cleanText === "不同意") {
      pendingReservationChanges.delete(orderCode);

      await replyGroupMention(
        event.replyToken,
        event.source.userId,
        "X"
      );

      await resetOrderForReDispatch(pending.order.order_id);

      queuePushText(
        DRIVER_GROUP_ID,
        `${pending.order.order_code} ${pending.reservationTime} ${pending.address}${pending.paymentText || ""}`,
        { merge: false }
      );

      return true;
    }
  }

  return false;
}

const port = process.env.PORT || 3000;

app.listen(port, () => {
  console.log("BOT RUNNING " + port);
});