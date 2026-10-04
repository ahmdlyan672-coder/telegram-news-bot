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

  ws.send(JSON.stringify({type:"message", message:text}));
  addLog("أنت: " + text);
  el.value = "";
}
</script>
</body>
</html>
"""

async def index(request):
    return web.Response(text=HTML, content_type="text/html")

async def health(request):
    return web.json_response({"ok": True, "rooms": len(rooms)})

async def ws_handler(request):
    room = (request.query.get("room") or "").strip().upper()

    if not room or len(room) > 64:
        return web.Response(status=400, text="Invalid room")

    ws = web.WebSocketResponse(heartbeat=25)
    await ws.prepare(request)

    members = rooms.setdefault(room, [])

    # الغرفة تدعم شخصين فقط
    if len(members) >= 2:
        await ws.send_json({
            "type": "error",
            "message": "الغرفة بيها شخصين بالفعل."
        })
        await ws.close()
        return ws

    members.append(ws)

    if len(members) == 1:
        await ws.send_json({
            "type": "status",
            "message": f"بانتظار الشخص الثاني. كود الغرفة: {room}"
        })
    elif len(members) == 2:
        for peer in list(members):
            if not peer.closed:
                await peer.send_json({
                    "type": "status",
                    "message": "تم ربط الطرفين."
                })

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue

                if data.get("type") != "message":
                    continue

                text = str(data.get("message", ""))[:5000]
                if not text:
                    continue

                # نرسل للطرف الآخر فقط
                for peer in list(rooms.get(room, [])):
                    if peer is not ws and not peer.closed:
                        await peer.send_json({
                            "type": "message",
                            "message": text
                        })

            elif msg.type == WSMsgType.ERROR:
                print("WebSocket error:", ws.exception())
                break

    finally:
        members = rooms.get(room, [])
        if ws in members:
            members.remove(ws)

        # نخبر الطرف الثاني أن شريكه خرج
        for peer in list(members):
            if not peer.closed:
                try:
                    await peer.send_json({
                        "type": "status",
                        "message": "الطرف الآخر خرج من الغرفة."
                    })
                except Exception:
                    pass

        if not members:
            rooms.pop(room, None)

    return ws

app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/health", health)
app.router.add_get("/ws", ws_handler)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    web.run_app(app, host="0.0.0.0", port=port)
