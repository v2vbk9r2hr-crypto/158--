const axios = require("axios");
const { supabase } = require("../config/supabase");

async function sendLineMessage(token, targetId, text) {
  await axios.post(
    "https://api.line.me/v2/bot/message/push",
    {
      to: targetId,
      messages: [{ type: "text", text }]
    },
    {
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json"
      },
      timeout: 10000
    }
  );
}

async function getActiveBots() {
  const { data, error } = await supabase
    .from("bot_accounts")
    .select("*")
    .eq("status", "active")
    .order("id", { ascending: true });

  if (error) {
    console.error("getActiveBots error:", error);
    return [];
  }

  return data || [];
}

async function getPendingJob() {
  const { data, error } = await supabase
    .from("message_jobs")
    .select("*")
    .eq("status", "pending")
    .order("priority", { ascending: true })
    .order("created_at", { ascending: true })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("getPendingJob error:", error);
    return null;
  }

  return data;
}

async function lockJob(jobId, botId) {
  const { data, error } = await supabase
    .from("message_jobs")
    .update({
      status: "sending",
      bot_id: botId,
      locked_at: new Date().toISOString()
    })
    .eq("id", jobId)
    .eq("status", "pending")
    .select()
    .maybeSingle();

  if (error) {
    console.error("lockJob error:", error);
    return null;
  }

  return data;
}

async function markJobSent(jobId) {
  await supabase
    .from("message_jobs")
    .update({
      status: "sent",
      sent_at: new Date().toISOString()
    })
    .eq("id", jobId);
}

async function markJobFailed(job, errorMessage) {
  const retryCount = (job.retry_count || 0) + 1;

  const nextStatus = retryCount >= 5 ? "failed" : "pending";

  await supabase
    .from("message_jobs")
    .update({
      status: nextStatus,
      retry_count: retryCount,
      bot_id: null,
      locked_at: null
    })
    .eq("id", job.id);

  console.error("message job failed:", {
    jobId: job.id,
    retryCount,
    nextStatus,
    errorMessage
  });
}

async function processOneJob() {
  const bots = await getActiveBots();
  if (bots.length === 0) return;

  const job = await getPendingJob();
  if (!job) return;

  for (const bot of bots) {
    const lockedJob = await lockJob(job.id, bot.id);
    if (!lockedJob) return;

    try {
      await sendLineMessage(
        bot.channel_access_token,
        lockedJob.target_id,
        lockedJob.message_text
      );

      await markJobSent(lockedJob.id);

      await supabase
        .from("bot_accounts")
        .update({ last_used_at: new Date().toISOString(), last_error: null })
        .eq("id", bot.id);

      console.log(`BOT ${bot.bot_name} sent job ${lockedJob.id}`);
      return;
    } catch (err) {
      const msg = err.response?.data || err.message;

      await supabase
        .from("bot_accounts")
        .update({ last_error: JSON.stringify(msg) })
        .eq("id", bot.id);

      await markJobFailed(lockedJob, JSON.stringify(msg));
      return;
    }
  }
}

function startBotWorker() {
  console.log("Bot Worker started");

  setInterval(() => {
    processOneJob().catch((err) => {
      console.error("Bot Worker error:", err);
    });
  }, 3000);
}

module.exports = {
  startBotWorker
};