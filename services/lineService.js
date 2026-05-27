async function replyText(client, replyToken, text) {
  return client.replyMessage(replyToken, {
    type: "text",
    text
  });
}

async function pushGroupMessage(client, groupId, text) {
  return client.pushMessage(groupId, {
    type: "text",
    text
  });
}

module.exports = {
  replyText,
  pushGroupMessage
};