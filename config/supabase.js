const { createClient } = require("@supabase/supabase-js");

const supabaseUrl =
  "https://lxxkopuejdayouaxyrwq.supabase.co";

const supabaseKey =
  process.env.SUPABASE_SERVICE_ROLE_KEY ||
  process.env.supabase_service_role_key ||
  process.env.SUPABASE_KEY ||
  process.env.supabase_key;

console.log("ENV:", Object.keys(process.env));

console.log("SUPABASE KEY EXISTS:", !!supabaseKey);

if (!supabaseKey) {
  throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY");
}

const supabase = createClient(
  supabaseUrl,
  supabaseKey.trim()
);

module.exports = supabase;