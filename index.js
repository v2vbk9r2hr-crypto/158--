require("dotenv").config();

const express = require("express");
const line = require("@line/bot-sdk");

const {
  createOrder,
  addDriverReport,
  decideWinner,
  getOrderByCodeAndAddress,
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

const pushQueue = [];
let isPushSending = false;

const PUSH_GAP_MS = 2500;        // 正常每 2.5 秒送一則
const MIN_PUSH_GAP_MS = 1800;    // 最快不低於 1.8 秒
const MAX_PUSH_GAP_MS = 8000;    // 被限流時最高拉到 8 秒
let currentPushGapMs = PUSH_GAP_MS;

const MERGE_WINDOW_MS = 1500;
const MAX_RETRY = 5;

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

function queuePushText(to, text, options = {}) {
  if (!to || !text) return;

  const now = Date.now();

  if (options.merge !== false && pushQueue.length > 0) {
    const lastJob = pushQueue[pushQueue.length - 1];

    if (
      lastJob &&
      lastJob.to === to &&
      lastJob.message.type === "text" &&
      now - lastJob.createdAt <= MERGE_WINDOW_MS
    ) {
      lastJob.message.text += "\n" + text;
      return;
    }
  }

  pushQueue.push({
    to,
    message: {
      type: "text",
      text
    },
    retry: 0,
    createdAt: now
  });

  processPushQueue();
}

async function processPushQueue() {
  if (isPushSending) return;

  isPushSending = true;

  while (pushQueue.length > 0) {
    const job = pushQueue.shift();

    try {
      await client.pushMessage(job.to, job.message);
      console.log("PUSH SEND:", job.message.text);

      // 成功後慢慢降回正常速度
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

        // 429 時自動降速
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

  while (pushQueue.length > 0) {
    const job = pushQueue.shift();

    try {
      await client.pushMessage(job.to, job.message);
      console.log("PUSH SEND:", job.message.text);
      await delay(PUSH_GAP_MS);
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
        const retryDelay = PUSH_GAP_MS * (job.retry + 2);
        await delay(retryDelay);
        pushQueue.unshift(job);
        continue;
      }

      await delay(PUSH_GAP_MS);
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

  console.log("Google 車程判斷：", {
    from: currentOrder.address,
    to: newAddress,
    googleMinutes,
    reportMinutes
  });

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

async function handleCustomerOrder(event, address) {
  if (address.length < 3) {
    return replyText(client, event.replyToken, "請輸入完整地址");
  }

  const order = await createOrder(address, event.source.userId);

  await replyText(
    client,
    event.replyToken,
    `立即為您派車，請稍等`
  );

  queuePushText(
    DRIVER_GROUP_ID,
    `${order.order_code} ${order.address}`
  );
}

async function handleDriverReport(event, text) {
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

        await client.pushMessage(DRIVER_GROUP_ID, {
  type: "textV2",
  text: "{driver} 噴",
  substitution: {
    driver: {
      type: "mention",
      mentionee: {
        type: "user",
        userId: winner.driver_line_id
      }
    }
  }
});

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

const port = process.env.PORT || 3000;

app.listen(port, () => {
  console.log("BOT RUNNING " + port);
});