let supabaseModule = require("./config/supabase");
const supabase = supabaseModule.supabase || supabaseModule;

function extractOrderCode(text) {
  const match = text.match(/#?[A-Z]\d{1,3}/i);
  return match ? match[0].replace("#", "").toUpperCase() : null;
}

function looksLikeOrder(text) {
  const hasOrderCode = /#?[A-Z]\d{1,3}/i.test(text);

  const hasAddress =
    /(台中|中區|東區|西區|南區|北區|西屯|南屯|北屯|大里|太平|烏日|豐原|潭子|路|街|巷|號|大道|段)/.test(text);

  const isClosedMessage =
    /(噴|已派|已搶|取|取消|客上|上車|抵達|到|X|失敗)/.test(text);

  return hasOrderCode && hasAddress && !isClosedMessage;
}

function looksClosed(text) {
  return /(噴|已派|已搶|取|取消)/.test(text);
}

async function handleDriverAssistant(event) {

    console.log("===== DRIVER ASSISTANT =====");

console.log("GROUP:",
  event?.source?.groupId
);

console.log("TEXT:",
  event?.message?.text
);

  try {
    if (!event || event.type !== "message") return;
    if (!event.message || event.message.type !== "text") return;
    if (!event.source || event.source.type !== "group") return;

    const text = event.message.text.trim();
    const groupId = event.source.groupId;
    const orderCode = extractOrderCode(text);

    if (!orderCode) return;

    if (looksLikeOrder(text)) {
      const { error } = await supabase
        .from("driver_assistant_orders")
        .upsert(
          {
            group_id: groupId,
            order_code: orderCode,
            raw_text: text,
            address: text,
            status: "open",
            detected_from: "line_group",
            updated_at: new Date().toISOString()
          },
          {
            onConflict: "group_id,order_code"
          }
        );

      if (error) {
        console.error("driverAssistant upsert error:", error);
      }

      return;
    }

    if (looksClosed(text)) {
      const status = /(取|取消)/.test(text) ? "canceled" : "closed";

      const { error } = await supabase
        .from("driver_assistant_orders")
        .update({
          status,
          closed_reason: text,
          closed_at: new Date().toISOString(),
          updated_at: new Date().toISOString()
        })
        .eq("group_id", groupId)
        .eq("order_code", orderCode)
        .eq("status", "open");

      if (error) {
        console.error("driverAssistant close error:", error);
      }
    }
  } catch (err) {
    console.error("driverAssistant fatal error:", err);
  }
}

async function cancelLatestCustomerOrder(customerLineId) {
  const { data, error } = await supabase
    .from("orders")
    .select("*")
    .eq("customer_line_id", customerLineId)
    .in("status", ["open", "assigned"])
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) throw error;
  if (!data) return null;

  const { data: updated, error: updateError } = await supabase
    .from("orders")
    .update({
      status: "canceled",
      canceled_at: new Date().toISOString()
    })
    .eq("order_id", data.order_id)
    .select("*")
    .maybeSingle();

  if (updateError) throw updateError;

  return updated;
}

module.exports = {
  handleDriverAssistant
};