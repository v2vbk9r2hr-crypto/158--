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

const config = {
  channelAccessToken:
    process.env.LINE_CHANNEL_ACCESS_TOKEN,

  channelSecret:
    process.env.LINE_CHANNEL_SECRET
};

const client =
  new line.Client(config);

const DRIVER_GROUP_ID =
  process.env.DRIVER_GROUP_ID;

// =========================
// 系統開關
// =========================

let BOT_ENABLED = true;
let REFRESH_ENABLED = true;

// =========================
// 派單規則
// =========================

const COMPETE_DIFF_MINUTES = 3;
const OVERRIDE_DIFF_MINUTES = 7;
const INSTANT_WIN_MINUTES = 5;
const GOOGLE_TOLERANCE_MINUTES = 3;

// =========================
// 429 防護
// =========================

const PUSH_GAP_MS = 4000;
const MIN_PUSH_GAP_MS = 3000;
const MAX_PUSH_GAP_MS = 15000;

const REFRESH_INTERVAL_MS =
  45 * 1000;

const REFRESH_BATCH_SIZE = 2;

const MAX_QUEUE_SIZE = 300;

const MAX_RETRY = 5;

// =========================
// Queue 分流
// =========================

const criticalQueue = [];
const refreshQueue = [];

let isPushSending = false;

let currentPushGapMs =
  PUSH_GAP_MS;

// =========================
// 熔斷保護
// =========================

let circuitBreaker = false;

let tooMany429Count = 0;

let pauseRefreshUntil = 0;

// =========================
// 冷卻保護
// =========================

const customerCooldown =
  new Map();

const driverCooldown =
  new Map();

// =========================
// 防重複
// =========================

const processingOrders =
  new Set();

const refreshingOrders =
  new Set();

const decidingOrders =
  new Set();

const pendingReservationChanges =
  new Map();

// =========================
// delay
// =========================

function delay(ms) {

  return new Promise(resolve =>
    setTimeout(resolve, ms)
  );
}

// =========================
// Error Helpers
// =========================

function getErrorStatus(err) {

  return (
    err?.statusCode ||
    err?.originalError?.response?.status
  );
}

function getErrorData(err) {

  return (
    err?.originalError?.response
      ?.data ||
    err.message
  );
}

// =========================
// Payment Detect
// =========================

function detectPaymentMethod(text) {

  const rules = [
    {
      keywords: [
        "客下街口",
        "下街口",
        "街口"
      ],
      value: "客下街口"
    },

    {
      keywords: [
        "客下轉帳"
      ],
      value: "客下轉帳"
    },

    {
      keywords: [
        "轉帳"
      ],
      value: "轉帳"
    },

    {
      keywords: [
        "現金"
      ],
      value: "現金"
    }
  ];

  for (const rule of rules) {

    for (
      const keyword of rule.keywords
    ) {

      if (
        text.includes(keyword)
      ) {

        return {
          keyword,
          value: rule.value
        };
      }
    }
  }

  return null;
}

function removePaymentKeyword(
  text,
  keyword
) {

  return text
    .replace(keyword, "")
    .replace(/\s+/g, " ")
    .trim();
}

// =========================
// Queue
// =========================

function addCriticalQueue(job) {

  const totalQueueSize =
    criticalQueue.length +
    refreshQueue.length;

  if (
    totalQueueSize >=
    MAX_QUEUE_SIZE
  ) {

    console.error(
      "QUEUE FULL"
    );

    return;
  }

  criticalQueue.push(job);

  processPushQueue();
}

function addRefreshQueue(job) {

  if (circuitBreaker) {
    return;
  }

  if (
    Date.now() <
    pauseRefreshUntil
  ) {
    return;
  }

  const totalQueueSize =
    criticalQueue.length +
    refreshQueue.length;

  if (
    totalQueueSize >=
    MAX_QUEUE_SIZE
  ) {

    console.error(
      "REFRESH DROP"
    );

    return;
  }

  refreshQueue.push(job);

  processPushQueue();
}

// =========================
// Push Processor
// =========================

async function processPushQueue() {

  if (isPushSending) return;

  isPushSending = true;

  while (
    criticalQueue.length > 0 ||
    refreshQueue.length > 0
  ) {

    let job;

    if (
      criticalQueue.length > 0
    ) {

      job =
        criticalQueue.shift();

    } else {

      job =
        refreshQueue.shift();
    }

    try {

      await client.pushMessage(
        job.to,
        job.message
      );

      console.log(
        "PUSH SEND:",
        job.message.text
      );

      currentPushGapMs =
        Math.max(
          MIN_PUSH_GAP_MS,
          currentPushGapMs - 300
        );

      await delay(
        currentPushGapMs
      );

    } catch (err) {

      const status =
        getErrorStatus(err);

      const data =
        getErrorData(err);

      console.error(
        "PUSH ERROR:",
        data
      );

      if (
        data &&
        typeof data ===
          "object" &&
        data.message ===
          "You have reached your monthly limit."
      ) {

        console.error(
          "LINE 月額度已用完"
        );

        criticalQueue.length = 0;
        refreshQueue.length = 0;

        break;
      }

      if (
        status === 429 &&
        job.retry < MAX_RETRY
      ) {

        job.retry += 1;

        tooMany429Count++;

        pauseRefreshUntil =
          Date.now() +
          5 * 60 * 1000;

        REFRESH_ENABLED =
          false;

        console.log(
          "429 停刷單5分鐘"
        );

        if (
          tooMany429Count >= 5
        ) {

          circuitBreaker =
            true;

          console.log(
            "熔斷保護啟動"
          );

          setTimeout(() => {

            circuitBreaker =
              false;

            tooMany429Count = 0;

            REFRESH_ENABLED =
              true;

            console.log(
              "熔斷解除"
            );

          }, 10 * 60 * 1000);
        }

        currentPushGapMs =
          Math.min(
            MAX_PUSH_GAP_MS,
            currentPushGapMs + 1500
          );

        const retryDelay =
          currentPushGapMs *
          job.retry;

        console.log(
          `429 ${retryDelay}ms 後重試`
        );

        await delay(
          retryDelay
        );

        if (
          job.priority ===
          "critical"
        ) {

          criticalQueue.unshift(
            job
          );

        } else {

          refreshQueue.unshift(
            job
          );
        }

        continue;
      }

      await delay(
        currentPushGapMs
      );
    }
  }

  isPushSending = false;
}

// =========================
// Queue Text
// =========================

function queueCriticalText(
  to,
  text
) {

  addCriticalQueue({
    to,
    priority: "critical",
    retry: 0,
    message: {
      type: "text",
      text
    }
  });
}

function queueRefreshText(
  to,
  text
) {

  addRefreshQueue({
    to,
    priority: "refresh",
    retry: 0,
    message: {
      type: "text",
      text
    }
  });
}

// =========================
// Mention
// =========================

async function replyMention(
  replyToken,
  userId,
  text
) {

  return client.replyMessage(
    replyToken,
    {
      type: "textV2",
      text:
        "{driver} " + text,

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
  );
}

// =========================
// Cooldown
// =========================

function checkCustomerCooldown(
  customerLineId
) {

  const last =
    customerCooldown.get(
      customerLineId
    );

  if (
    last &&
    Date.now() - last < 5000
  ) {

    return false;
  }

  customerCooldown.set(
    customerLineId,
    Date.now()
  );

  return true;
}

function checkDriverCooldown(
  driverLineId
) {

  const last =
    driverCooldown.get(
      driverLineId
    );

  if (
    last &&
    Date.now() - last < 3000
  ) {

    return false;
  }

  driverCooldown.set(
    driverLineId,
    Date.now()
  );

  return true;
}

// =========================
// Bot Control
// =========================

async function handleBotControl(
  event,
  text
) {

  if (
    event.source.type !==
    "group"
  ) {
    return false;
  }

  if (
    text ===
      "停止機器人運作" ||
    text === "停止"
  ) {

    BOT_ENABLED = false;

    await setBotSetting(
      "bot_enabled",
      "false"
    );

    processingOrders.clear();
    refreshingOrders.clear();
    decidingOrders.clear();

    criticalQueue.length = 0;
    refreshQueue.length = 0;

    await client.replyMessage(
      event.replyToken,
      {
        type: "text",
        text:
          "機器人已停止運作"
      }
    );

    return true;
  }

  if (
    text ===
      "開始機器人運作" ||
    text === "開始"
  ) {

    BOT_ENABLED = true;

    await setBotSetting(
      "bot_enabled",
      "true"
    );

    await client.replyMessage(
      event.replyToken,
      {
        type: "text",
        text:
          "機器人已開始運作"
      }
    );

    return true;
  }

  if (text === "停止刷單") {

    REFRESH_ENABLED = false;

    await setBotSetting(
      "refresh_enabled",
      "false"
    );

    await client.replyMessage(
      event.replyToken,
      {
        type: "text",
        text:
          "刷單功能已停止"
      }
    );

    return true;
  }

  if (text === "開始刷單") {

    REFRESH_ENABLED = true;

    await setBotSetting(
      "refresh_enabled",
      "true"
    );

    await client.replyMessage(
      event.replyToken,
      {
        type: "text",
        text:
          "刷單功能已開始"
      }
    );

    return true;
  }

  return false;
}

// =========================
// Webhook
// =========================

app.post(
  "/webhook",
  line.middleware(config),

  async (req, res) => {

    res.status(200).end();

    const events =
      req.body.events || [];

    for (const event of events) {

      handleEvent(event).catch(
        err => {

          console.error(
            "handleEvent error:",
            err
          );
        }
      );
    }
  }
);

// =========================
// Event
// =========================

async function handleEvent(
  event
) {

  if (
    event.type !==
    "message"
  ) {
    return;
  }

  if (
    event.message.type !==
    "text"
  ) {
    return;
  }

  const text =
    event.message.text.trim();

  const controlled =
    await handleBotControl(
      event,
      text
    );

  if (controlled) {
    return;
  }

  if (!BOT_ENABLED) {
    return;
  }

  if (
    event.source.type ===
    "user"
  ) {

    return handleCustomerOrder(
      event,
      text
    );
  }

  if (
    event.source.type ===
    "group"
  ) {

    return handleDriverReport(
      event,
      text
    );
  }
}

// =========================
// Customer Order
// =========================

async function handleCustomerOrder(
  event,
  addressText
) {

  const customerLineId =
    event.source.userId;

  if (
    !checkCustomerCooldown(
      customerLineId
    )
  ) {

    return client.replyMessage(
      event.replyToken,
      {
        type: "text",
        text:
          "請稍後再試"
      }
    );
  }

  try {

    if (
      processingOrders.has(
        customerLineId
      )
    ) {

      return;
    }

    processingOrders.add(
      customerLineId
    );

    // =========================
    // 取消
    // =========================

    if (
      addressText ===
        "取消" ||
      addressText ===
        "取消叫車" ||
      addressText ===
        "取" ||
      addressText ===
        "不用車"
    ) {

      const canceledOrder =
        await cancelLatestCustomerOrder(
          customerLineId
        );

      processingOrders.delete(
        customerLineId
      );

      if (!canceledOrder) {

        return client.replyMessage(
          event.replyToken,
          {
            type: "text",
            text:
              "目前沒有可取消的訂單"
          }
        );
      }

      if (
        canceledOrder.assigned_driver_line_id
      ) {

        queueCriticalText(
          DRIVER_GROUP_ID,
          `@司機 取`
        );
      }

      return client.replyMessage(
        event.replyToken,
        {
          type: "text",
          text:
            "已取消叫車"
        }
      );
    }

    // =========================
    // 付款方式
    // =========================

    const paymentDetected =
      detectPaymentMethod(
        addressText
      );

    if (paymentDetected) {

      await upsertCustomerPreference(
        customerLineId,
        paymentDetected.value
      );

      addressText =
        removePaymentKeyword(
          addressText,
          paymentDetected.keyword
        );
    }

    // =========================
    // 建立訂單
    // =========================

    const order =
      await createOrder(
        addressText,
        customerLineId
      );

    const preference =
      await getCustomerPreference(
        customerLineId
      );

    const paymentText =
      preference &&
      preference.payment_method
        ? ` ${preference.payment_method}`
        : "";

    processingOrders.delete(
      customerLineId
    );

    await client.replyMessage(
      event.replyToken,
      {
        type: "text",
        text:
          "立即為您派車"
      }
    );

    queueCriticalText(
      DRIVER_GROUP_ID,
      `${order.order_code} ${order.address}${paymentText}`
    );

  } catch (err) {

    processingOrders.delete(
      customerLineId
    );

    console.error(
      "handleCustomerOrder error:",
      err
    );
  }
}

// =========================
// Driver Report
// =========================

async function handleDriverReport(
  event,
  text
) {

  if (
    !checkDriverCooldown(
      event.source.userId
    )
  ) {

    return;
  }

  const parsed =
    parseDriverMessage(text);

  if (!parsed) {
    return;
  }

  const {
    orderCode,
    address,
    plate,
    minutes
  } = parsed;

  const order =
    await getOrderByCodeAndAddress(
      orderCode,
      address
    );

  if (!order) {
    return;
  }

  if (
    order.status ===
    "canceled"
  ) {

    return replyMention(
      event.replyToken,
      event.source.userId,
      "X"
    );
  }

  // =========================
  // 已有司機
  // =========================

  if (
    order.status ===
    "assigned"
  ) {

    const oldArrival =
      new Date(
        order.assigned_at
      ).getTime() +
      Number(
        order.assigned_minutes
      ) *
        60 *
        1000;

    const newArrival =
      Date.now() +
      Number(minutes) *
        60 *
        1000;

    const diffMinutes =
      (oldArrival -
        newArrival) /
      1000 /
      60;

    if (
      diffMinutes <
      OVERRIDE_DIFF_MINUTES
    ) {

      return replyMention(
        event.replyToken,
        event.source.userId,
        "X"
      );
    }

    const updatedOrder =
      await overrideDriver({
        order,
        driverLineId:
          event.source.userId,
        plate,
        minutes
      });

    queueCriticalText(
      DRIVER_GROUP_ID,
      "@司機 噴"
    );

    queueCriticalText(
      updatedOrder.customer_line_id,
      `司機已出發\n車牌:${plate}\n約${minutes}分鐘`
    );

    return;
  }

  // =========================
  // 新增回報
  // =========================

  await addDriverReport({
    orderId: order.order_id,
    orderCode,
    address,
    driverLineId:
      event.source.userId,
    plate,
    minutes
  });

  // =========================
  // 5分鐘直接中選
  // =========================

  if (
    Number(minutes) <=
    INSTANT_WIN_MINUTES
  ) {

    const updatedOrder =
      await overrideDriver({
        order,
        driverLineId:
          event.source.userId,
        plate,
        minutes
      });

    await replyMention(
      event.replyToken,
      event.source.userId,
      "噴"
    );

    queueCriticalText(
      updatedOrder.customer_line_id,
      `司機已出發\n車牌:${plate}\n約${minutes}分鐘`
    );

    return;
  }

  // =========================
  // 自動判斷
  // =========================

  if (
    !order.decision_started &&
    !decidingOrders.has(
      order.order_id
    )
  ) {

    await assignWinnerDriver(
      order.order_id
    );

    decidingOrders.add(
      order.order_id
    );

    setTimeout(async () => {

      try {

        const result =
          await decideWinner(
            order.order_id
          );

        if (!result) {
          return;
        }

        const {
          order:
            assignedOrder,
          winner
        } = result;

        queueCriticalText(
          DRIVER_GROUP_ID,
          "@司機 噴"
        );

        queueCriticalText(
          assignedOrder.customer_line_id,
          `司機已出發\n車牌:${winner.plate}\n約${winner.minutes}分鐘`
        );

      } catch (err) {

        console.error(
          "decideWinner error:",
          err
        );

      } finally {

        decidingOrders.delete(
          order.order_id
        );
      }

    }, 10000);
  }
}

// =========================
// 刷單
// =========================

async function refreshOpenOrders() {

  if (!BOT_ENABLED) {
    return;
  }

  if (!REFRESH_ENABLED) {
    return;
  }

  if (circuitBreaker) {
    return;
  }

  if (
    Date.now() <
    pauseRefreshUntil
  ) {
    return;
  }

  try {

    const orders =
      await getOpenOrdersForRefresh();

    if (
      !orders ||
      orders.length === 0
    ) {
      return;
    }

    const refreshTargets =
      orders.slice(
        0,
        REFRESH_BATCH_SIZE
      );

    for (const order of refreshTargets) {

      if (
        refreshingOrders.has(
          order.order_id
        )
      ) {

        continue;
      }

      refreshingOrders.add(
        order.order_id
      );

      const preference =
        await getCustomerPreference(
          order.customer_line_id
        );

      const paymentText =
        preference &&
        preference.payment_method
          ? ` ${preference.payment_method}`
          : "";

      queueRefreshText(
        DRIVER_GROUP_ID,
        `${order.order_code} ${order.address}${paymentText}`
      );

      await markOrderRefreshed(
        order.order_id
      );

      refreshingOrders.delete(
        order.order_id
      );

      await delay(1000);
    }

  } catch (err) {

    refreshingOrders.clear();

    console.error(
      "refreshOpenOrders error:",
      err
    );
  }
}

// =========================
// Load Setting
// =========================

async function loadBotSettings() {

  const botEnabled =
    await getBotSetting(
      "bot_enabled"
    );

  const refreshEnabled =
    await getBotSetting(
      "refresh_enabled"
    );

  BOT_ENABLED =
    botEnabled !== "false";

  REFRESH_ENABLED =
    refreshEnabled !== "false";

  console.log(
    "BOT_ENABLED:",
    BOT_ENABLED
  );

  console.log(
    "REFRESH_ENABLED:",
    REFRESH_ENABLED
  );
}

// =========================
// Start
// =========================

setInterval(
  refreshOpenOrders,
  REFRESH_INTERVAL_MS
);

app.get("/", (req, res) => {

  res.send(
    `BOT=${BOT_ENABLED} REFRESH=${REFRESH_ENABLED}`
  );
});

const port =
  process.env.PORT || 3000;

loadBotSettings().then(() => {

  app.listen(port, () => {

    console.log(
      "BOT RUNNING:",
      port
    );
  });
});