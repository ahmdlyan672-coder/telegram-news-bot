import json
import os
from aiohttp import web, WSMsgType

rooms = {}

HTML = r"""
<!doctype html>
<html lang="ar" dir="rtl">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Relay Voice Chat</title>
  <style>
    body {
      font-family: Arial, sans-serif;
      max-width: 820px;
      margin: 35px auto;
      padding: 0 16px;
      background: #111;
      color: #eee;
    }
    h2 { margin-bottom: 8px; }
    input, button {
      padding: 12px;
      margin: 6px 0;
      font-size: 16px;
      box-sizing: border-box;
    }
    input { width: 100%; }
    button { cursor: pointer; }
    .row {
      display: flex;
      gap: 8px;
      align-items: center;
      flex-wrap: wrap;
    }
    .row input { flex: 1; min-width: 220px; }
    #log {
      background: #1b1b1b;
      border: 1px solid #333;
      min-height: 260px;
      max-height: 360px;
      padding: 12px;
      overflow: auto;
      white-space: pre-wrap;
      margin-top: 8px;
    }
    .controls button { min-width: 120px; }
    #status {
      padding: 8px 0;
      font-weight: bold;
    }
    audio { width: 100%; margin-top: 8px; }
  </style>
</head>
<body>

  <h2>سيرفر الربط الصوتي</h2>
  <p>أدخل نفس كود الغرفة على الجهازين.</p>

  <div class="row">
    <input id="room" placeholder="كود الغرفة مثل ABC123">
    <button onclick="randomRoom()">إنشاء كود</button>
  </div>

  <div class="row controls">
    <button onclick="connectWS()">اتصال بالغرفة</button>
    <button onclick="startCall()">بدء مكالمة صوتية</button>
    <button onclick="toggleMute()" id="muteBtn">كتم المايك</button>
    <button onclick="hangup()">إنهاء المكالمة</button>
  </div>

  <div id="status">غير متصل</div>

  <div id="log"></div>

  <div class="row">
    <input id="msg" placeholder="اكتب رسالة..." onkeydown="if(event.key==='Enter') sendMsg()">
    <button onclick="sendMsg()">إرسال</button>
  </div>

  <audio id="remoteAudio" autoplay playsinline></audio>

<script>
let ws = null;
let pc = null;
let localStream = null;
let isMuted = false;
let peerReady = false;

const rtcConfig = {
  iceServers: [
    { urls: "stun:stun.l.google.com:19302" },
    { urls: "stun:stun1.l.google.com:19302" }
  ]
};

function addLog(t) {
  const log = document.getElementById("log");
  log.textContent += t + "\n";
  log.scrollTop = log.scrollHeight;
}

function setStatus(t) {
  document.getElementById("status").textContent = t;
}

function randomRoom() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let s = "";
  for (let i = 0; i < 8; i++) {
    s += chars[Math.floor(Math.random() * chars.length)];
  }
  document.getElementById("room").value = s;
}

function sendSignal(payload) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    addLog("مو متصل بالسيرفر.");
    return;
  }

  ws.send(JSON.stringify({
    type: "signal",
    payload
  }));
}

async function connectWS() {
  const room = document.getElementById("room").value.trim();

  if (!room) {
    alert("أدخل كود الغرفة");
    return;
  }

  if (ws && ws.readyState === WebSocket.OPEN) {
    addLog("أنت متصل بالفعل.");
    return;
  }

  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(
    `${proto}://${location.host}/ws?room=${encodeURIComponent(room)}`
  );

  ws.onopen = () => {
    addLog("تم الاتصال بالسيرفر.");
    setStatus("متصل بالسيرفر");
  };

  ws.onclose = () => {
    addLog("انقطع الاتصال بالسيرفر.");
    setStatus("غير متصل");
    peerReady = false;
  };

  ws.onerror = () => {
    addLog("حدث خطأ بالاتصال.");
  };

  ws.onmessage = async (ev) => {
    let d;

    try {
      d = JSON.parse(ev.data);
    } catch {
      return;
    }

    if (d.type === "status") {
      addLog("[حالة] " + d.message);

      if (d.code === "paired") {
        peerReady = true;
        setStatus("تم ربط الطرفين");
      }

      if (d.code === "peer_left") {
        peerReady = false;
        setStatus("الطرف الآخر خرج");
        cleanupPeerConnection();
      }
    }

    else if (d.type === "message") {
      addLog("الطرف الآخر: " + d.message);
    }

    else if (d.type === "signal") {
      await handleSignal(d.payload);
    }

    else if (d.type === "error") {
      addLog("[خطأ] " + d.message);
    }
  };
}

async function ensureLocalAudio() {
  if (localStream) return localStream;

  try {
    localStream = await navigator.mediaDevices.getUserMedia({
      audio: true,
      video: false
    });

    addLog("تم السماح باستخدام المايك.");
    return localStream;
  } catch (e) {
    addLog("تعذر الوصول للمايك: " + e.message);
    throw e;
  }
}

async function createPeerConnection() {
  if (pc) return pc;

  pc = new RTCPeerConnection(rtcConfig);

  pc.onicecandidate = (event) => {
    if (event.candidate) {
      sendSignal({
        kind: "ice",
        candidate: event.candidate
      });
    }
  };

  pc.ontrack = (event) => {
    const audio = document.getElementById("remoteAudio");
    audio.srcObject = event.streams[0];
    audio.play().catch(() => {});
    addLog("تم استلام الصوت من الطرف الآخر.");
  };

  pc.onconnectionstatechange = () => {
    setStatus("حالة المكالمة: " + pc.connectionState);

    if (pc.connectionState === "connected") {
      addLog("المكالمة الصوتية متصلة.");
    }

    if (
      pc.connectionState === "failed" ||
      pc.connectionState === "disconnected" ||
      pc.connectionState === "closed"
    ) {
      addLog("انقطع اتصال المكالمة.");
    }
  };

  const stream = await ensureLocalAudio();

  for (const track of stream.getTracks()) {
    pc.addTrack(track, stream);
  }

  return pc;
}

async function startCall() {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    alert("اتصل بالغرفة أولاً.");
    return;
  }

  if (!peerReady) {
    alert("انتظر إلى أن يدخل الطرف الثاني.");
    return;
  }

  try {
    await createPeerConnection();

    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);

    sendSignal({
      kind: "offer",
      sdp: pc.localDescription
    });

    addLog("تم إرسال طلب المكالمة.");
    setStatus("جاري بدء المكالمة...");
  } catch (e) {
    addLog("فشل بدء المكالمة: " + e.message);
  }
}

async function handleSignal(payload) {
  if (!payload || !payload.kind) return;

  try {
    if (payload.kind === "offer") {
      await createPeerConnection();

      await pc.setRemoteDescription(
        new RTCSessionDescription(payload.sdp)
      );

      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);

      sendSignal({
        kind: "answer",
        sdp: pc.localDescription
      });

      addLog("تم قبول المكالمة.");
    }

    else if (payload.kind === "answer") {
      if (!pc) return;

      await pc.setRemoteDescription(
        new RTCSessionDescription(payload.sdp)
      );

      addLog("الطرف الآخر قبل المكالمة.");
    }

    else if (payload.kind === "ice") {
      await createPeerConnection();

      if (payload.candidate) {
        try {
          await pc.addIceCandidate(
            new RTCIceCandidate(payload.candidate)
          );
        } catch (e) {
          addLog("ICE error: " + e.message);
        }
      }
    }

    else if (payload.kind === "hangup") {
      addLog("الطرف الآخر أنهى المكالمة.");
      cleanupPeerConnection();
      setStatus("تم إنهاء المكالمة");
    }

  } catch (e) {
    addLog("خطأ WebRTC: " + e.message);
  }
}

function toggleMute() {
  if (!localStream) {
    addLog("شغّل المكالمة أولاً.");
    return;
  }

  isMuted = !isMuted;

  for (const track of localStream.getAudioTracks()) {
    track.enabled = !isMuted;
  }

  document.getElementById("muteBtn").textContent =
    isMuted ? "تشغيل المايك" : "كتم المايك";

  addLog(isMuted ? "تم كتم المايك." : "تم تشغيل المايك.");
}

function cleanupPeerConnection() {
  if (pc) {
    try {
      pc.onicecandidate = null;
      pc.ontrack = null;
      pc.close();
    } catch {}
    pc = null;
  }

  document.getElementById("remoteAudio").srcObject = null;
}

function hangup() {
  if (pc) {
    sendSignal({ kind: "hangup" });
  }

  cleanupPeerConnection();

  if (localStream) {
    for (const track of localStream.getTracks()) {
      track.stop();
    }
    localStream = null;
  }

  isMuted = false;
  document.getElementById("muteBtn").textContent = "كتم المايك";
  setStatus("تم إنهاء المكالمة");
  addLog("تم إنهاء المكالمة.");
}

function sendMsg() {
  const el = document.getElementById("msg");
  const text = el.value.trim();

  if (!text) return;

  if (!ws || ws.readyState !== WebSocket.OPEN) {
    addLog("مو متصل بالسيرفر.");
    return;
  }

  ws.send(JSON.stringify({
    type: "message",
    message: text
  }));

  addLog("أنت: " + text);
  el.value = "";
}

window.addEventListener("beforeunload", () => {
  try {
    if (ws) ws.close();
  } catch {}
});
</script>
</body>
</html>
"""

async def index(request):
    return web.Response(text=HTML, content_type="text/html")

async def health(request):
    return web.json_response({
        "ok": True,
        "rooms": len(rooms)
    })

async def send_to_peer(room, sender, payload):
    for peer in list(rooms.get(room, [])):
        if peer is not sender and not peer.closed:
            await peer.send_json(payload)

async def ws_handler(request):
    room = (request.query.get("room") or "").strip().upper()

    if not room or len(room) > 64:
        return web.Response(status=400, text="Invalid room")

    ws = web.WebSocketResponse(heartbeat=25)
    await ws.prepare(request)

    members = rooms.setdefault(room, [])

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
            "code": "waiting",
            "message": f"بانتظار الشخص الثاني. كود الغرفة: {room}"
        })

    elif len(members) == 2:
        for peer in list(members):
            if not peer.closed:
                await peer.send_json({
                    "type": "status",
                    "code": "paired",
                    "message": "تم ربط الطرفين."
                })

    try:
        async for msg in ws:

            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue

                msg_type = data.get("type")

                if msg_type == "message":
                    text = str(data.get("message", ""))[:5000]

                    if text:
                        await send_to_peer(room, ws, {
                            "type": "message",
                            "message": text
                        })

                elif msg_type == "signal":
                    payload = data.get("payload")

                    if isinstance(payload, dict):
                        await send_to_peer(room, ws, {
                            "type": "signal",
                            "payload": payload
                        })

            elif msg.type == WSMsgType.ERROR:
                break

    finally:
        members = rooms.get(room, [])

        if ws in members:
            members.remove(ws)

        for peer in list(members):
            if not peer.closed:
                try:
                    await peer.send_json({
                        "type": "status",
                        "code": "peer_left",
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
