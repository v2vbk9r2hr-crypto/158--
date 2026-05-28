// ========================================
// TAXI BOT 商用完整版 V3
// 第一階段 + 第二階段 + 訊息防火牆整合版
// ========================================

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

const {
  parseDriverMessage
} = require("./utils/parser");

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

// ========================================
// 訊息防火牆
// ========================================

const strictDriverRegex =
  /^#?[A-Z]\d\/\s+.+\s+[A-Za-z0-9\-]+\s+\d+$/;

// ========================================
// 系統開關
// ========================================

let BOT_ENABLED = true;
let REFRESH_ENABLED = true;

// ========================================
// Queue
// ========================================

const criticalQueue = [];
const normalQueue = [];
const refreshQueue = [];

let isPushSending = false;

// ========================================
// 429 防護
// ========================================

const PUSH_GAP_MS = 4000;
const MIN_PUSH_GAP_MS = 3000;
const MAX_PUSH_GAP_MS = 15000;

let currentPushGapMs =
  PUSH_GAP_MS;

const MAX_QUEUE_SIZE = 300;
const MAX_RETRY = 5;

// ========================================
// 熔斷保護
// ========================================

let circuitBreaker = false;

let tooMany429Count = 0;

let pauseRefreshUntil = 0;

// ========================================
// 刷單
// ========================================

const REFRESH_INTERVAL_MS =
  45 * 1000;

const REFRESH_BATCH_SIZE = 2;

// ========================================
// 規則
// ========================================

const COMPETE_DIFF_MINUTES = 3;
const OVERRIDE_DIFF_MINUTES = 7;
const INSTANT_WIN_MINUTES = 5;

// ========================================
// 防重複
// ========================================

const processingOrders =
  new Set();

const refreshingOrders =
  new Set();

const decidingOrders =
  new Set();

const pendingReservationChanges =
  new Map();

// ========================================
// 冷卻
// ========================================

const customerCooldown =
  new Map();

const driverCooldown =
  new Map();

// ========================================
// 黑名單
// ========================================

const blacklistCustomers =
  new Set();

// ========================================
// 取消費
// ========================================

const cancelFees =
  new Map();

// ========================================
// 統計
// ========================================

let totalOrders = 0;
let total429 = 0;
let totalCanceled = 0;
let totalAssigned = 0;

// ========================================
// delay
// ========================================

function delay(ms) {

  return new Promise(resolve =>
    setTimeout(resolve, ms)
  );
}

// ========================================
// Error
// ========================================

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

// ========================================
// Payment
// ========================================

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

// ========================================
// Queue Add
// ========================================

function addQueue(
  queue,
  job
) {

  const totalSize =
    criticalQueue.length +
    normalQueue.length +
    refreshQueue.length;

  if (
    totalSize >=
    MAX_QUEUE_SIZE
  ) {

    console.error(
      "QUEUE FULL"
    );

    return;
  }

  queue.push(job);

  processPushQueue();
}

function queueCriticalText(
  to,
  text
) {

  addQueue(
    criticalQueue,
    {
      to,
      retry: 0,
      priority: "critical",

      message: {
        type: "text",
        text
      }
    }
  );
}

function queueNormalText(
  to,
  text
) {

  addQueue(
    normalQueue,
    {
      to,
      retry: 0,
      priority: "normal",

      message: {
        type: "text",
        text
      }
    }
  );
}

function queueRefreshText(
  to,
  text
) {

  if (circuitBreaker) {
    return;
  }

  if (
    Date.now() <
    pauseRefreshUntil
  ) {
    return;
  }

  addQueue(
    refreshQueue,
    {
      to,
      retry: 0,
      priority: "refresh",

      message: {
        type: "text",
        text
      }
    }
  );
}

// ========================================
// Push Processor
// ========================================

async function processPushQueue() {

  if (isPushSending) return;

  isPushSending = true;

  while (
    criticalQueue.length > 0 ||
    normalQueue.length > 0 ||
    refreshQueue.length > 0
  ) {

    let job;

    if (
      criticalQueue.length > 0
    ) {

      job =
        criticalQueue.shift();

    } else if (
      normalQueue.length > 0
    ) {

      job =
        normalQueue.shift();

    } else {

      job =
        refreshQueue.shift();
    }

    try {

      await client.pushMessage(
        job.to,
        job.message
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

        criticalQueue.length = 0;
        normalQueue.length = 0;
        refreshQueue.length = 0;

        break;
      }

      if (
        status === 429 &&
        job.retry < MAX_RETRY
      ) {

        total429++;

        job.retry += 1;

        tooMany429Count++;

        pauseRefreshUntil =
          Date.now() +
          5 * 60 * 1000;

        REFRESH_ENABLED =
          false;

        if (
          tooMany429Count >= 5
        ) {

          circuitBreaker =
            true;

          setTimeout(() => {

            circuitBreaker =
              false;

            tooMany429Count = 0;

            REFRESH_ENABLED =
              true;

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

        } else if (
          job.priority ===
          "normal"
        ) {

          normalQueue.unshift(
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

// ========================================
// Mention
// ========================================

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

// ========================================
// Cooldown
// ========================================

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

// ========================================
// Bot Control
// ========================================

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

    criticalQueue.length = 0;
    normalQueue.length = 0;
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

  if (text === "系統狀態") {

    await client.replyMessage(
      event.replyToken,
      {
        type: "text",
        text:
`BOT:${BOT_ENABLED}
REFRESH:${REFRESH_ENABLED}
429:${total429}
總訂單:${totalOrders}
已取消:${totalCanceled}
已派送:${totalAssigned}
Critical:${criticalQueue.length}
Normal:${normalQueue.length}
Refresh:${refreshQueue.length}`
      }
    );

    return true;
  }

  return false;
}

// ========================================
// Webhook
// ========================================

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

// ========================================
// Event 防火牆版
// ========================================

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

  if (!text) {
    return;
  }

  if (text.length < 2) {
    return;
  }

  // ========================================
  // 群組
  // ========================================

  if (
    event.source.type ===
    "group"
  ) {

    if (
      event.source.groupId !==
      DRIVER_GROUP_ID
    ) {

      return;
    }

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
      !checkDriverCooldown(
        event.source.userId
      )
    ) {

      return;
    }

    // ========================================
    // 非 # 開頭忽略
    // ========================================

    if (
      !text.startsWith("#")
    ) {

      console.log(
        "非搶單訊息忽略:",
        text
      );

      return;
    }

    // ========================================
    // 非法格式忽略
    // ========================================

    if (
      !strictDriverRegex.test(
        text
      )
    ) {

      console.log(
        "非法格式忽略:",
        text
      );

      return;
    }

    return handleDriverReport(
      event,
      text
    );
  }

  // ========================================
  // 客人
  // ========================================

  if (
    event.source.type ===
    "user"
  ) {

    if (!BOT_ENABLED) {
      return;
    }

    return handleCustomerOrder(
      event,
      text
    );
  }
}

// ========================================
// Customer
// ========================================

async function handleCustomerOrder(
  event,
  addressText
) {

  const customerLineId =
    event.source.userId;

  if (
    blacklistCustomers.has(
      customerLineId
    )
  ) {

    return client.replyMessage(
      event.replyToken,
      {
        type: "text",
        text:
          "您目前無法使用叫車"
      }
    );
  }

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

    // ========================================
    // 取消
    // ========================================

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

      const currentFee =
        cancelFees.get(
          customerLineId
        ) || 0;

      cancelFees.set(
        customerLineId,
        currentFee + 100
      );

      totalCanceled++;

      return client.replyMessage(
        event.replyToken,
        {
          type: "text",
          text:
`已取消叫車
取消費:${cancelFees.get(customerLineId)}`
        }
      );
    }

    // ========================================
    // 付款方式
    // ========================================

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

    // ========================================
    // 建立訂單
    // ========================================

    const order =
      await createOrder(
        addressText,
        customerLineId
      );

    totalOrders++;

    const preference =
      await getCustomerPreference(
        customerLineId
      );

    const paymentText =
      preference &&
      preference.payment_method
        ? ` ${preference.payment_method}`
        : "";

    const fee =
      cancelFees.get(
        customerLineId
      ) || 0;

    const feeText =
      fee > 0
        ? ` 代收取消費${fee}`
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
      `${order.order_code} ${order.address}${paymentText}${feeText}`
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

// ========================================
// Driver
// ========================================

async function handleDriverReport(
  event,
  text
) {

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

  await addDriverReport({
    orderId: order.order_id,
    orderCode,
    address,
    driverLineId:
      event.source.userId,
    plate,
    minutes
  });

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

    totalAssigned++;

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
}

// ========================================
// 刷單
// ========================================

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

      queueRefreshText(
  DRIVER_GROUP_ID,
  "🪳---🪳 我是分隔線 🪳---🪳"
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
  "----------我是分隔線----------"
);

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

// ========================================
// Auto Resume
// ========================================

setInterval(() => {

  if (
    !REFRESH_ENABLED &&
    !circuitBreaker &&
    Date.now() >
      pauseRefreshUntil
  ) {

    REFRESH_ENABLED = true;
  }

}, 60 * 1000);

// ========================================
// Queue Monitor
// ========================================

setInterval(() => {

  console.log(
`Critical:${criticalQueue.length}
Normal:${normalQueue.length}
Refresh:${refreshQueue.length}
429:${total429}`
  );

}, 5 * 60 * 1000);

// ========================================
// Start
// ========================================

setInterval(
  refreshOpenOrders,
  REFRESH_INTERVAL_MS
);

app.get("/", (req, res) => {

  res.send(
    `BOT=${BOT_ENABLED}`
  );
});

const port =
  process.env.PORT || 3000;

app.listen(port, () => {

  console.log(
    "BOT RUNNING:",
    port
  );
});