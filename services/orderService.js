```js
const supabase = require("../config/supabase");

function makeOrderCode() {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  const letter = chars[Math.floor(Math.random() * chars.length)];
  const number = Math.floor(Math.random() * 9) + 1;
  return `#${letter}${number}/`;
}

async function createOrder(address, customerLineId) {
  const orderCode = makeOrderCode();

  const { data, error } = await supabase
    .from("orders")
    .insert({
      order_code: orderCode,
      address,
      customer_line_id: customerLineId,
      status: "open",
      decision_started: false
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
      decision_started: false
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

module.exports = {
  createOrder,
  getOrderByCodeAndAddress,
  getLatestCustomerOrder,
  addDriverReport,
  getFirstDriverReport,
  assignWinnerDriver,
  decideWinner,
  overrideDriver,
  resetOrderForReDispatch,
  getDriverCurrentOrder,
  upsertDriverCurrentOrder
};
```
