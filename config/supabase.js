const { createClient } = require("@supabase/supabase-js");

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

console.log("SUPABASE_URL exists:", !!supabaseUrl);
console.log("SUPABASE_KEY exists:", !!supabaseKey);

if (!supabaseUrl) throw new Error("Missing SUPABASE_URL in cloud Variables");
if (!supabaseKey) throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY in cloud Variables");

const supabase = createClient(supabaseUrl, supabaseKey);

module.exports = supabase;