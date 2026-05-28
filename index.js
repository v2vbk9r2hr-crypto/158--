// ========================================
// 多官方帳號 + 全功能 + 防429 + 刷單 + 分隔線
// 官方A + 官方B + 同一司機群
// 完整商用版
// ========================================

require("dotenv").config();

const express = require("express");
const line = require("@line/bot-sdk");

const {
  createOrder,
  addDriverReport,
  decideWinner,
  getOrderByCodeAndAddress,
  upsertCustomerPreference,
  getCustomerPreference,
  getOpenOrdersForRefresh,
  markOrderRefreshed,
  cancelLatestCustomerOrder,
  assignWinnerDriver,
  overrideDriver
} = require("./services/orderService");

const {
  parseDriverMessage
} = require("./utils/parser");

const app = express();

// ========================================
// 官方A
// ========================================

const configA = {
  channelAccessToken:
    process.env.LINE_CHANNEL_ACCESS_TOKEN,

  channelSecret:
    process.env.LINE_CHANNEL_SECRET
};

const clientA =
  new line.Client(configA);

// ========================================
// 官方B（安全啟動版）
// ========================================

let clientB = null;

const hasLineB =
  process.env.LINE_B_CHANNEL_ACCESS_TOKEN &&
  process.env.LINE_B_CHANNEL_SECRET;

if (hasLineB) {

  const configB = {
    channelAccessToken:
      process.env.LINE_B_CHANNEL_ACCESS_TOKEN,

    channelSecret:
      process.env.LINE_B_CHANNEL_SECRET
  };

  clientB =
    new line.Client(configB);

  console.log(
    "官方B 已啟用"
  );

} else {

  console.log(
    "官方B 尚未啟用"
  );
}

// ========================================
// 基本設定
// ========================================

const DRIVER_GROUP_ID =
  process.env.DRIVER_GROUP_ID;

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
// 熔斷
// ========================================

let circuitBreaker = false;

let tooMany429Count = 0;

let pauseRefreshUntil = 0;

// ========================================
// 刷單
// ========================================

const REFRESH_INTERVAL_MS =
  45 * 1000;

const REFRESH_BATCH_SIZE = 3;

// ========================================
// 規則
// ========================================

const INSTANT_WIN_MINUTES = 5;

// ========================================
// 防重複
// ========================================

const processingOrders =
  new Set();

const refreshingOrders =
  new Set();

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
// 防火牆
// ========================================

const strictDriverRegex =
  /^#?[A-Z]\d\/\s+.+\s+[A-Za-z0-9\-]+\s+\d+$/;

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
// Queue
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
  text,
  source = "A"
) {

  addQueue(
    criticalQueue,
    {
      to,
      retry: 0,
      source,
      priority: "critical",

      message: {
        type: "text",
        text
      }
    }
  );
}

function queueRefreshText(
  to,
  text,
  source = "A"
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
      source,
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

      // ====================================
      // 官方A
      // ====================================

      if (
        job.source === "A"
      ) {

        await clientA.pushMessage(
          job.to,
          job.message
        );

      } else {

        // ====================================
        // 官方B
        // ====================================

        if (!clientB) {
          continue;
        }

        await clientB.pushMessage(
          job.to,
          job.message
        );
      }

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
        status === 429 &&
        job.retry < MAX_RETRY
      ) {

        total429++;

        job.retry += 1;

        tooMany429Count++;

        REFRESH_ENABLED =
          false;

        pauseRefreshUntil =
          Date.now() +
          5 * 60 * 1000;

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

        await delay(
          currentPushGapMs *
          job.retry
        );

        criticalQueue.unshift(
          job
        );

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
// Reply
// ========================================

async function replyText(
  clientObj,
  replyToken,
  text
) {

  return clientObj.replyMessage(
    replyToken,
    {
      type: "text",
      text
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
  text,
  clientObj
) {

  if (
    event.source.type !==
    "group"
  ) {
    return false;
  }

  // ====================================
  // 停止機器人
  // ====================================

  if (
    text ===
      "停止機器人運作" ||
    text === "停止"
  ) {

    BOT_ENABLED = false;

    criticalQueue.length = 0;
    normalQueue.length = 0;
    refreshQueue.length = 0;

    await replyText(
      clientObj,
      event.replyToken,
      "機器人已停止運作"
    );

    return true;
  }

  // ====================================
  // 開始機器人
  // ====================================

  if (
    text ===
      "開始機器人運作" ||
    text === "開始"
  ) {

    BOT_ENABLED = true;

    await replyText(
      clientObj,
      event.replyToken,
      "機器人已開始運作"
    );

    return true;
  }

  // ====================================
  // 停止刷單
  // ====================================

  if (
    text === "停止刷單"
  ) {

    REFRESH_ENABLED = false;

    await replyText(
      clientObj,
      event.replyToken,
      "刷單功能已停止"
    );

    return true;
  }

  // ====================================
  // 開始刷單
  // ====================================

  if (
    text === "開始刷單"
  ) {

    REFRESH_ENABLED = true;

    await replyText(
      clientObj,
      event.replyToken,
      "刷單功能已開始"
    );

    return true;
  }

  // ====================================
  // 系統狀態
  // ====================================

  if (
    text === "系統狀態"
  ) {

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

// ========================================
// 官方A webhook
// ========================================

app.post(
  "/webhook",

  line.middleware(configA),

  async (req, res) => {

    res.status(200).end();

    const events =
      req.body.events || [];

    for (const event of events) {

      handleEvent(
        event,
        clientA,
        "A"
      ).catch(err => {

        console.error(
          "A error:",
          err
        );
      });
    }
  }
);

// ========================================
// 官方B webhook
// ========================================

if (hasLineB) {

  const configB = {
    channelAccessToken:
      process.env.LINE_B_CHANNEL_ACCESS_TOKEN,

    channelSecret:
      process.env.LINE_B_CHANNEL_SECRET
  };

  app.post(
    "/webhook-b",

    line.middleware(configB),

    async (req, res) => {

      res.status(200).end();

      const events =
        req.body.events || [];

      for (const event of events) {

        handleEvent(
          event,
          clientB,
          "B"
        ).catch(err => {

          console.error(
            "B error:",
            err
          );
        });
      }
    }
  );
}

// ========================================
// Event
// ========================================

async function handleEvent(
  event,
  clientObj,
  source
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

    if (
      !text.startsWith("#")
    ) {

      return;
    }

    if (
      !strictDriverRegex.test(
        text
      )
    ) {

      console.log(
        "非法格式:",
        text
      );

      return;
    }

    return handleDriverReport(
      event,
      text,
      source
    );
  }

  // ========================================
  // 客人
  // ========================================

  if (
    event.source.type ===
    "user"
  ) {

    return handleCustomerOrder(
      event,
      text,
      clientObj,
      source
    );
  }
}

// ========================================
// Customer
// ========================================

async function handleCustomerOrder(
  event,
  addressText,
  clientObj,
  source
) {

  const customerLineId =
    event.source.userId;

  if (
    blacklistCustomers.has(
      customerLineId
    )
  ) {

    return replyText(
      clientObj,
      event.replyToken,
      "您目前無法使用叫車"
    );
  }

  if (
    !checkCustomerCooldown(
      customerLineId
    )
  ) {

    return replyText(
      clientObj,
      event.replyToken,
      "請稍後再試"
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
        "取"
    ) {

      await cancelLatestCustomerOrder(
        customerLineId
      );

      processingOrders.delete(
        customerLineId
      );

      totalCanceled++;

      return replyText(
        clientObj,
        event.replyToken,
        "已取消叫車"
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

    if (
      addressText.length < 3
    ) {

      processingOrders.delete(
        customerLineId
      );

      return replyText(
        clientObj,
        event.replyToken,
        "請輸入完整地址"
      );
    }

    // ========================================
    // 建立訂單
    // ========================================

    const order =
      await createOrder(
        addressText,
        customerLineId,
        source
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

    processingOrders.delete(
      customerLineId
    );

    await replyText(
      clientObj,
      event.replyToken,
      "立即為您派車"
    );

    queueCriticalText(
      DRIVER_GROUP_ID,
      `${order.order_code} ${order.address}${paymentText}`,
      source
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
  text,
  source
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

  // ========================================
  // 5分鐘內直接中選
  // ========================================

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
      "@司機 噴",
      source
    );

    queueCriticalText(
      updatedOrder.customer_line_id,
`司機已出發
車牌:${plate}
約${minutes}分鐘`,
      source
    );

    return;
  }

  // ========================================
  // 自動判斷
  // ========================================

  if (
    !order.decision_started
  ) {

    await assignWinnerDriver(
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
          order: assignedOrder,
          winner
        } = result;

        totalAssigned++;

        queueCriticalText(
          DRIVER_GROUP_ID,
          "@司機 噴",
          source
        );

        queueCriticalText(
          assignedOrder.customer_line_id,
`司機已出發
車牌:${winner.plate}
約${winner.minutes}分鐘`,
          source
        );

      } catch (err) {

        console.error(
          "decideWinner error:",
          err
        );
      }

    }, 10000);
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

    // ========================================
    // 分隔線（只刷一次）
    // ========================================

    queueRefreshText(
      DRIVER_GROUP_ID,
      "🪳---🪳 我是分隔線 🪳---🪳"
    );

    // ========================================
    // 開始刷單
    // ========================================

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
        `${order.order_code} ${order.address}${paymentText}`,
        order.source_name || "A"
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
// 自動恢復
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

  console.log(
    "LINE_B_SECRET exists:",
    !!process.env.LINE_B_CHANNEL_SECRET
  );

  console.log(
    "LINE_B_TOKEN exists:",
    !!process.env.LINE_B_CHANNEL_ACCESS_TOKEN
  );
});