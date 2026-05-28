const supabase = require("../config/supabase");

function makeOrderCode() {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  const letter = chars[Math.floor(Math.random() * chars.length)];
  const number = Math.floor(Math.random() * 9) + 1;
  return `#${letter}${number}/`;
}

async function createOrder(address, customerLineId, source = "A") {
  const orderCode = makeOrderCode();

  const { data, error } = await supabase
    .from("orders")
    .insert({
      order_code: orderCode,
      address,
      customer_line_id: customerLineId,
      status: "open",
      decision_started: false,
      source_name: source
    })
    .select()
    .single();

  if (error) {
    console.error("createOrder error:", error);
    throw error;
  }

  return data;
}

async function getOrderByCodeAndAddress(orderCode, address) {
  const { data, error } = await supabase
    .from("orders")
    .select("*")
    .eq("order_code", orderCode)
    .eq("address", address)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("getOrderByCodeAndAddress error:", error);
    return null;
  }

  return data;
}

async function getLatestCustomerOrder(customerLineId) {
  const { data, error } = await supabase
    .from("orders")
    .select("*")
    .eq("customer_line_id", customerLineId)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("getLatestCustomerOrder error:", error);
    return null;
  }

  return data;
}

async function upsertCustomerPreference(customerLineId, paymentMethod) {
  const { data, error } = await supabase
    .from("customer_preferences")
    .upsert(
      {
        customer_line_id: customerLineId,
        payment_method: paymentMethod,
        updated_at: new Date().toISOString()
      },
      { onConflict: "customer_line_id" }
    )
    .select()
    .single();

  if (error) {
    console.error("upsertCustomerPreference error:", error);
    return null;
  }

  return data;
}

async function getCustomerPreference(customerLineId) {
  const { data, error } = await supabase
    .from("customer_preferences")
    .select("*")
    .eq("customer_line_id", customerLineId)
    .maybeSingle();

  if (error) {
    console.error("getCustomerPreference error:", error);
    return null;
  }

  return data;
}

async function addDriverReport({
  orderId,
  orderCode,
  address,
  driverLineId,
  plate,
  minutes
}) {
  const { data, error } = await supabase
    .from("driver_reports")
    .insert({
      order_id: orderId,
      order_code: orderCode,
      address,
      driver_line_id: driverLineId,
      plate,
      minutes
    })
    .select()
    .single();

  if (error) {
    console.error("addDriverReport error:", error);
    throw error;
  }

  return data;
}

async function getFirstDriverReport(orderId) {
  const { data, error } = await supabase
    .from("driver_reports")
    .select("*")
    .eq("order_id", orderId)
    .order("created_at", { ascending: true })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("getFirstDriverReport error:", error);
    return null;
  }

  return data;
}

async function assignWinnerDriver(orderId) {
  const { data, error } = await supabase
    .from("orders")
    .update({ decision_started: true })
    .eq("order_id", orderId)
    .select()
    .single();

  if (error) {
    console.error("assignWinnerDriver error:", error);
    throw error;
  }

  return data;
}

async function decideWinner(orderId) {
  const { data: reports, error } = await supabase
    .from("driver_reports")
    .select("*")
    .eq("order_id", orderId)
    .order("minutes", { ascending: true })
    .order("created_at", { ascending: true });

  if (error || !reports || reports.length === 0) return null;

  const winner = reports[0];

  const { data: order, error: updateError } = await supabase
    .from("orders")
    .update({
      status: "assigned",
      assigned_driver_line_id: winner.driver_line_id,
      assigned_plate: winner.plate,
      assigned_minutes: winner.minutes,
      assigned_at: new Date().toISOString()
    })
    .eq("order_id", orderId)
    .select()
    .single();

  if (updateError) {
    console.error("decideWinner update error:", updateError);
    return null;
  }

  return { order, winner };
}

async function overrideDriver({ order, driverLineId, plate, minutes }) {
  const { data, error } = await supabase
    .from("orders")
    .update({
      status: "assigned",
      assigned_driver_line_id: driverLineId,
      assigned_plate: plate,
      assigned_minutes: minutes,
      assigned_at: new Date().toISOString()
    })
    .eq("order_id", order.order_id)
    .select()
    .single();

  if (error) {
    console.error("overrideDriver error:", error);
    throw error;
  }

  return data;
}

async function resetOrderForReDispatch(orderId) {
  const { data, error } = await supabase
    .from("orders")
    .update({
      status: "open",
      assigned_driver_line_id: null,
      assigned_plate: null,
      assigned_minutes: null,
      assigned_at: null,
      decision_started: false,
      last_refreshed_at: null
    })
    .eq("order_id", orderId)
    .select()
    .single();

  if (error) {
    console.error("resetOrderForReDispatch error:", error);
    throw error;
  }

  return data;
}

async function getOpenOrdersForRefresh() {
  const refreshAgo = new Date(Date.now() - 3 * 60 * 1000).toISOString();

  const { data, error } = await supabase
    .from("orders")
    .select("*")
    .eq("status", "open")
    .eq("decision_started", false)
    .or(`last_refreshed_at.is.null,last_refreshed_at.lt.${refreshAgo}`)
    .order("created_at", { ascending: true });

  if (error) {
    console.error("getOpenOrdersForRefresh error:", error);
    return [];
  }

  return data || [];
}

async function markOrderRefreshed(orderId) {
  const { error } = await supabase
    .from("orders")
    .update({
      last_refreshed_at: new Date().toISOString()
    })
    .eq("order_id", orderId);

  if (error) {
    console.error("markOrderRefreshed error:", error);
  }
}

async function cancelLatestCustomerOrder(customerLineId) {
  const latestOrder = await getLatestCustomerOrder(customerLineId);

  if (!latestOrder) return null;

  if (latestOrder.status !== "open" && latestOrder.status !== "assigned") {
    return null;
  }

  const { data, error } = await supabase
    .from("orders")
    .update({
      status: "canceled",
      canceled_at: new Date().toISOString(),
      decision_started: false
    })
    .eq("order_id", latestOrder.order_id)
    .select()
    .single();

  if (error) {
    console.error("cancelLatestCustomerOrder error:", error);
    return null;
  }

  return data;
}

async function getDriverCurrentOrder(driverLineId) {
  const { data, error } = await supabase
    .from("driver_current_orders")
    .select("*")
    .eq("driver_line_id", driverLineId)
    .maybeSingle();

  if (error) {
    console.error("getDriverCurrentOrder error:", error);
    return null;
  }

  return data;
}

async function upsertDriverCurrentOrder({
  driverLineId,
  orderId,
  orderCode,
  address,
  plate,
  status = "assigned"
}) {
  const { data, error } = await supabase
    .from("driver_current_orders")
    .upsert({
      driver_line_id: driverLineId,
      order_id: orderId,
      order_code: orderCode,
      address,
      plate,
      status,
      updated_at: new Date().toISOString()
    })
    .select()
    .single();

  if (error) {
    console.error("upsertDriverCurrentOrder error:", error);
    return null;
  }

  return data;
}

async function getBotSetting(key) {
  const { data, error } = await supabase
    .from("bot_settings")
    .select("*")
    .eq("key", key)
    .maybeSingle();

  if (error) {
    console.error("getBotSetting error:", error);
    return null;
  }

  return data?.value || null;
}

async function setBotSetting(key, value) {
  const { data, error } = await supabase
    .from("bot_settings")
    .upsert(
      {
        key,
        value: String(value),
        updated_at: new Date().toISOString()
      },
      { onConflict: "key" }
    )
    .select()
    .single();

  if (error) {
    console.error("setBotSetting error:", error);
    return null;
  }

  return data;
}

module.exports = {
  createOrder,
  getOrderByCodeAndAddress,
  getLatestCustomerOrder,
  upsertCustomerPreference,
  getCustomerPreference,
  addDriverReport,
  getFirstDriverReport,
  assignWinnerDriver,
  decideWinner,
  overrideDriver,
  resetOrderForReDispatch,
  getOpenOrdersForRefresh,
  markOrderRefreshed,
  cancelLatestCustomerOrder,
  getDriverCurrentOrder,
  upsertDriverCurrentOrder,
  getBotSetting,
  setBotSetting
};