const { createClient } = require("@supabase/supabase-js");

const supabaseUrl =
  process.env.SUPABASE_URL ||
  "https://lkopuejdayouaxyrwq.supabase.co";

const supabaseKey =
  process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseKey) {
  throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY");
}

const supabase = createClient(supabaseUrl.trim(), supabaseKey.trim());

module.exports = supabase;