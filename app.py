import asyncio
import json
import os
from aiohttp import web, WSMsgType

# الغرف بالشكل:
# rooms["ROOMCODE"] = [websocket1, websocket2]
rooms = {}

HTML = r"""
<!doctype html>
<html lang="ar" dir="rtl">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Relay Chat</title>
  <style>
    body { font-family: Arial, sans-serif; max-width: 760px; margin: 40px auto; padding: 0 16px; background:#111; color:#eee; }
    input, button { padding: 12px; margin: 6px 0; font-size:16px; }
    input { width:100%; box-sizing:border-box; }
    button { cursor:pointer; }
    #log { background:#1b1b1b; border:1px solid #333; min-height:260px; padding:12px; overflow:auto; white-space:pre-wrap; }
    .row { display:flex; gap:8px; }
    .row input { flex:1; }
  </style>
</head>
<body>
  <h2>سيرفر الربط</h2>
  <p>أدخل نفس كود الغرفة على الجهازين.</p>

  <div class="row">
    <input id="room" placeholder="كود الغرفة مثل ABC123">
    <button onclick="randomRoom()">إنشاء كود</button>
  </div>

  <button onclick="connectWS()">اتصال</button>

  <div id="log"></div>

  <div class="row">
    <input id="msg" placeholder="اكتب رسالة..." onkeydown="if(event.key==='Enter') sendMsg()">
    <button onclick="sendMsg()">إرسال</button>
  </div>

<script>
let ws = null;

function addLog(t) {
  const log = document.getElementById("log");
  log.textContent += t + "\n";
  log.scrollTop = log.scrollHeight;
}

function randomRoom() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let s = "";
  for (let i = 0; i < 8; i++) s += chars[Math.floor(Math.random()*chars.length)];
  document.getElementById("room").value = s;
}

function connectWS() {
  const room = document.getElementById("room").value.trim();
  if (!room) {
    alert("أدخل كود الغرفة");
    return;
  }

  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws?room=${encodeURIComponent(room)}`);

  ws.onopen = () => addLog("تم الاتصال بالسيرفر.");
  ws.onclose = () => addLog("انقطع الاتصال.");
  ws.onerror = () => addLog("حدث خطأ بالاتصال.");

  ws.onmessage = (ev) => {
    try {
      const d = JSON.parse(ev.data);
      if (d.type === "status") addLog("[حالة] " + d.message);
      else if (d.type === "message") addLog("الطرف الآخر: " + d.message);
      else if (d.type === "error") addLog("[خطأ] " + d.message);
    } catch {
      addLog(ev.data);
    }
  };
}

function sendMsg() {
  const el = document.getElementById("msg");
  const text = el.value.trim();
  if (!text) return;

  if (!ws || ws.readyState !== WebSocket.OPEN) {
    addLog("مو متصل بالسيرفر.");
    return;
  }
