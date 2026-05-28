const { createClient } = require("@supabase/supabase-js");

const supabaseUrl =
  "https://https://lxxkopuejdayouaxyrwq.supabase.co";

const supabaseKey =
  process.env.SUPABASE_SERVICE_ROLE_KEY ||
  process.env.supabase_service_role_key;

if (!supabaseKey) {
  throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY");
}

const supabase = createClient(
  supabaseUrl,
  supabaseKey.trim()
);

module.exports = supabase;