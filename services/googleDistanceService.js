const axios = require("axios");

async function getDrivingMinutes(fromAddress, toAddress) {
  try {
    const key = process.env.GOOGLE_MAPS_API_KEY;

    if (!key) {
      console.error("缺少 GOOGLE_MAPS_API_KEY");
      return null;
    }

    const { data } = await axios.get(
      "https://maps.googleapis.com/maps/api/distancematrix/json",
      {
        params: {
          origins: fromAddress,
          destinations: toAddress,
          mode: "driving",
          language: "zh-TW",
          region: "tw",
          key
        }
      }
    );

    const element = data.rows?.[0]?.elements?.[0];

    if (!element || element.status !== "OK") {
      console.error("Google Distance error:", data);
      return null;
    }

    return Math.ceil(element.duration.value / 60);
  } catch (err) {
    console.error("getDrivingMinutes error:", err.message);
    return null;
  }
}

module.exports = {
  getDrivingMinutes
};