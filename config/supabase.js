const { createClient } = require("@supabase/supabase-js");

console.log("ENV keys:", Object.keys(process.env).filter(k => k.toLowerCase().includes("supabase")));

const supabaseUrl =
  process.env.SUPABASE_URL ||
  process.env.supabase_url ||
  "https://lkopuejdayouaxyrwq.supabase.co";

const supabaseKey =
  process.env.SUPABASE_SERVICE_ROLE_KEY ||
  process.env.supabase_service_role_key ||
  process.env.SUPABASE_KEY ||
  process.env.supabase_key;

console.log("supabaseUrl exists:", !!supabaseUrl);
console.log("supabaseKey exists:", !!supabaseKey);

if (!supabaseKey) {
  throw new Error("Missing Supabase key. Check Railway Variables name/value.");
}

const supabase = createClient(supabaseUrl.trim(), supabaseKey.trim());

module.exports = supabase;