const crypto = require("crypto");
const { supabase } = require("../config/supabase");

function makeOrderId() {
  return crypto.randomUUID();
}

async function createOrder(address, customerLineId, source = "A") {
  const sourceName = source || "A";
  const orderId = makeOrderId();

  const { data: nextNo, error: counterError } = await supabase
    .rpc("next_order_number", {
      p_source_name: sourceName
    });

  if (counterError) throw counterError;

  const orderCode = `#${sourceName}${nextNo}/`;

  const { data, error } = await supabase
    .from("orders")
    .insert([{
      order_id: orderId,
      order_code: orderCode,
      address,
      customer_line_id: customerLineId,
      source_name: sourceName,
      status: "waiting",
      reservation_locked: false
    }])
    .select()
    .single();

  if (error) throw error;
  return data;
}

async function getOrderByCodeAndAddress(orderCode, address) {
  const { data, error } = await supabase
    .from("orders")
    .select("*")
    .eq("order_code", orderCode)
    .eq("address", address)
    .neq("status", "completed")
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) throw error;
  if (data) return data;

  return getOrderByCode(orderCode);
}

async function getOrderByCode(orderCode) {
  const { data, error } = await supabase
    .from("orders")
    .select("*")
    .eq("order_code", orderCode)
    .neq("status", "completed")
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) throw error;
  return data;
}

async function lockReservationWinner(orderId, driverLineId, status) {
  const { data, error } = await supabase
    .from("orders")
    .update({
      reservation_locked: true,
      reservation_driver: driverLineId,
      reservation_status: status
    })
    .eq("order_id", orderId)
    .select()
    .single();

  if (error) throw error;
  return data;
}

async function getReservationLock(orderId) {
  const { data, error } = await supabase
    .from("orders")
    .select("reservation_locked,reservation_driver,reservation_status")
    .eq("order_id", orderId)
    .maybeSingle();

  if (error) throw error;
  return data;
}

async function getLatestCustomerOrder(customerLineId) {
  const { data, error } = await supabase
    .from("orders")
    .select("*")
    .eq("customer_line_id", customerLineId)
    .in("status", ["waiting", "assigned", "arrived", "customer_on"])
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) throw error;
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

  if (error) throw error;
  return data;
}

async function getCustomerPreference(customerLineId) {
  const { data, error } = await supabase
    .from("customer_preferences")
    .select("*")
    .eq("customer_line_id", customerLineId)
    .maybeSingle();

  if (error) throw error;
  return data;
}

async function addDriverReport({ orderId, orderCode, address, driverLineId, plate, minutes }) {
  const { data, error } = await supabase
    .from("driver_reports")
    .insert([{
      order_id: orderId,
      order_code: orderCode,
      address,
      driver_line_id: driverLineId,
      plate,
      minutes
    }])
    .select()
    .single();

  if (error) throw error;
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

  if (error) throw error;
  return data;
}

async function assignWinnerDriver(orderId) {
  const { data, error } = await supabase
    .from("orders")
    .update({ decision_started: true })
    .eq("order_id", orderId)
    .select()
    .single();

  if (error) throw error;
  return data;
}

async function decideWinner(orderId) {
  const { data: reports, error: reportError } = await supabase
    .from("driver_reports")
    .select("*")
    .eq("order_id", orderId)
    .order("minutes", { ascending: true })
    .order("created_at", { ascending: true });

  if (reportError) throw reportError;
  if (!reports || reports.length === 0) return null;

  const winner = reports[0];

  const { data: order, error: orderError } = await supabase
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

  if (orderError) throw orderError;

  const losers = reports.filter(
  r => r.driver_line_id !== winner.driver_line_id
);

return {
  order,
  winner,
  losers
};
}

async function overrideDriver({ order, driverLineId, plate, minutes }) {
  const { data, error } = await supabase
    .from("orders")
    .update({
      status: "assigned",
      assigned_driver_line_id: driverLineId,
      assigned_plate: plate,
      assigned_minutes: minutes,
      assigned_at: new Date().toISOString(),
      decision_started: true
    })
    .eq("order_id", order.order_id)
    .select()
    .single();

  if (error) throw error;
  return data;
}

async function resetOrderForReDispatch(orderId) {
  const { data, error } = await supabase
    .from("orders")
    .update({
      status: "waiting",
      assigned_driver_line_id: null,
      assigned_plate: null,
      assigned_minutes: null,
      assigned_at: null,
      decision_started: false,
      reservation_locked: false,
      reservation_driver: null,
      reservation_status: null
    })
    .eq("order_id", orderId)
    .select()
    .single();

  if (error) throw error;
  return data;
}

async function getOpenOrdersForRefresh() {
  const { data, error } = await supabase
    .from("orders")
    .select("*")
    .eq("status", "waiting")
    .order("last_refreshed_at", { ascending: true, nullsFirst: true })
    .order("created_at", { ascending: true })
    .limit(50);

  if (error) throw error;
  return data || [];
}

async function markOrderRefreshed(orderId) {
  const { data, error } = await supabase
    .from("orders")
    .update({
      last_refreshed_at: new Date().toISOString()
    })
    .eq("order_id", orderId)
    .select()
    .single();

  if (error) throw error;
  return data;
}

async function getDriverCurrentOrder(driverLineId) {
  const { data, error } = await supabase
    .from("driver_current_orders")
    .select("*")
    .eq("driver_line_id", driverLineId)
    .maybeSingle();

  if (error) throw error;
  return data;
}

async function upsertDriverCurrentOrder({ driverLineId, orderId, orderCode, address, plate, status = "assigned" }) {
  const { data, error } = await supabase
    .from("driver_current_orders")
    .upsert(
      {
        driver_line_id: driverLineId,
        order_id: orderId,
        order_code: orderCode,
        address,
        plate,
        status,
        updated_at: new Date().toISOString()
      },
      { onConflict: "driver_line_id" }
    )
    .select()
    .single();

  if (error) throw error;
  return data;
}

async function getBotSetting(key) {
  const { data, error } = await supabase
    .from("bot_settings")
    .select("*")
    .eq("key", key)
    .maybeSingle();

  if (error) throw error;
  return data ? data.value : null;
}

async function setBotSetting(key, value) {
  const { data, error } = await supabase
    .from("bot_settings")
    .upsert(
      {
        key,
        value,
        updated_at: new Date().toISOString()
      },
      { onConflict: "key" }
    )
    .select()
    .single();

  if (error) throw error;
  return data;
}

module.exports = {
  createOrder,
  getOrderByCodeAndAddress,
  getOrderByCode,
  lockReservationWinner,
  getReservationLock,
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