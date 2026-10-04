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
  <title>سجاد ال علي - Voice Connect</title>
  <style>
    * { box-sizing: border-box; }

    :root{
      --bg1:#07111f;
      --bg2:#101a35;
      --glass:rgba(255,255,255,.08);
      --glass-strong:rgba(255,255,255,.12);
      --border:rgba(255,255,255,.13);
      --text:#f7f9ff;
      --muted:#aeb8d6;
      --primary:#7c5cff;
      --primary2:#00d4ff;
      --success:#33e1a5;
      --danger:#ff5470;
      --warning:#ffcb6b;
      --shadow:0 25px 80px rgba(0,0,0,.42);
    }

    html,body{
      margin:0;
      min-height:100%;
      font-family:"Segoe UI", Tahoma, Arial, sans-serif;
      color:var(--text);
      background:
        radial-gradient(circle at 15% 15%, rgba(124,92,255,.30), transparent 35%),
        radial-gradient(circle at 85% 10%, rgba(0,212,255,.22), transparent 34%),
        radial-gradient(circle at 50% 100%, rgba(51,225,165,.16), transparent 35%),
        linear-gradient(145deg,var(--bg1),var(--bg2));
      background-attachment: fixed;
    }

    body{
      padding:28px 16px 42px;
      overflow-x:hidden;
    }

    .orb{
      position:fixed;
      width:250px;height:250px;
      border-radius:50%;
      filter:blur(55px);
      opacity:.18;
      pointer-events:none;
      z-index:0;
    }
    .orb.one{top:4%;left:3%;background:#7c5cff}
    .orb.two{top:24%;right:1%;background:#00d4ff}
    .orb.three{bottom:0;left:35%;background:#33e1a5}

    .wrap{
      position:relative;
      z-index:1;
      width:min(980px,100%);
      margin:auto;
    }

    .hero{
      text-align:center;
      margin-bottom:22px;
      padding:26px 20px 16px;
    }

    .brand{
      display:inline-flex;
      align-items:center;
      gap:10px;
      padding:9px 16px;
      border:1px solid var(--border);
      background:rgba(255,255,255,.06);
      backdrop-filter:blur(16px);
      border-radius:999px;
      color:var(--muted);
      font-size:14px;
      box-shadow:0 10px 30px rgba(0,0,0,.18);
    }

    .brand-dot{
      width:10px;height:10px;border-radius:50%;
      background:linear-gradient(135deg,var(--primary2),var(--success));
      box-shadow:0 0 15px rgba(0,212,255,.85);
    }

    h1{
      margin:18px 0 8px;
      font-size:clamp(32px,6vw,58px);
      line-height:1.05;
      letter-spacing:-1px;
      background:linear-gradient(90deg,#fff,#b9c7ff,#7cf1ff);
      -webkit-background-clip:text;
      background-clip:text;
      color:transparent;
      text-shadow:0 0 35px rgba(124,92,255,.18);
    }

    .subtitle{
      color:var(--muted);
      font-size:16px;
      margin:0;
    }

    .grid{
      display:grid;
      grid-template-columns:1.05fr .95fr;
      gap:18px;
    }

    @media(max-width:820px){
      .grid{grid-template-columns:1fr}
    }

    .card{
      border:1px solid var(--border);
      background:linear-gradient(180deg,rgba(255,255,255,.10),rgba(255,255,255,.055));
      backdrop-filter:blur(22px);
      border-radius:26px;
      box-shadow:var(--shadow);
      overflow:hidden;
    }

    .card-head{
      padding:20px 22px 10px;
    }

    .card-title{
      display:flex;
      align-items:center;
      gap:10px;
      font-weight:800;
      font-size:18px;
    }

    .card-sub{
      color:var(--muted);
      font-size:13px;
      margin-top:5px;
    }

    .card-body{
      padding:14px 22px 22px;
    }

    .room-box{
      display:flex;
      gap:10px;
      align-items:center;
    }

    .input{
      width:100%;
      border:1px solid rgba(255,255,255,.12);
      outline:none;
      color:#fff;
      background:rgba(4,10,25,.55);
      border-radius:16px;
      padding:14px 15px;
      font-size:16px;
      transition:.2s ease;
    }

    .input:focus{
      border-color:rgba(0,212,255,.65);
      box-shadow:0 0 0 4px rgba(0,212,255,.08);
    }

    .btn{
      border:0;
      outline:0;
      color:#fff;
      font-weight:800;
      cursor:pointer;
      border-radius:15px;
      padding:13px 16px;
      transition:.18s ease;
      box-shadow:0 8px 22px rgba(0,0,0,.18);
      white-space:nowrap;
    }

    .btn:hover{transform:translateY(-2px)}
    .btn:active{transform:translateY(0) scale(.98)}

    .btn-primary{
      background:linear-gradient(135deg,var(--primary),#9d65ff);
    }
    .btn-cyan{
      background:linear-gradient(135deg,#00a9ff,var(--primary2));
    }
    .btn-green{
      background:linear-gradient(135deg,#1dbb86,var(--success));
      color:#07251b;
    }
    .btn-red{
      background:linear-gradient(135deg,#ff4569,var(--danger));
    }
    .btn-ghost{
      background:rgba(255,255,255,.08);
      border:1px solid rgba(255,255,255,.12);
    }

    .controls{
      display:grid;
      grid-template-columns:1fr 1fr;
      gap:10px;
      margin-top:12px;
    }

    @media(max-width:520px){
      .room-box{flex-direction:column}
      .room-box .btn{width:100%}
      .controls{grid-template-columns:1fr}
    }

    .status-card{
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:12px;
      margin-top:16px;
      padding:14px 15px;
      border-radius:17px;
      background:rgba(255,255,255,.055);
      border:1px solid rgba(255,255,255,.1);
    }

    .status-left{
      display:flex;
      align-items:center;
      gap:11px;
    }

    .pulse{
      width:12px;height:12px;border-radius:50%;
      background:#77809c;
      box-shadow:0 0 0 0 rgba(119,128,156,.35);
      transition:.2s;
    }

    .pulse.online{
      background:var(--success);
      box-shadow:0 0 18px rgba(51,225,165,.8);
      animation:pulse 1.8s infinite;
    }

    @keyframes pulse{
      0%{box-shadow:0 0 0 0 rgba(51,225,165,.42)}
      70%{box-shadow:0 0 0 12px rgba(51,225,165,0)}
      100%{box-shadow:0 0 0 0 rgba(51,225,165,0)}
    }

    #status{
      font-weight:800;
      font-size:14px;
    }

    .small{
      color:var(--muted);
      font-size:12px;
    }

    .log{
      height:360px;
      overflow:auto;
      padding:14px;
      border-radius:18px;
      background:rgba(3,8,20,.55);
      border:1px solid rgba(255,255,255,.08);
      scroll-behavior:smooth;
    }

    .bubble{
      max-width:82%;
      padding:10px 12px;
      margin:8px 0;
      border-radius:14px;
      line-height:1.5;
      word-wrap:break-word;
      animation:fade .18s ease;
    }

    .bubble.me{
      margin-right:auto;
      background:linear-gradient(135deg,rgba(124,92,255,.95),rgba(112,75,231,.95));
      border-bottom-right-radius:5px;
    }

    .bubble.other{
      margin-left:auto;
      background:rgba(255,255,255,.10);
      border:1px solid rgba(255,255,255,.08);
      border-bottom-left-radius:5px;
    }

    .bubble.system{
      max-width:100%;
      text-align:center;
      color:var(--muted);
      background:transparent;
      font-size:13px;
      padding:5px;
    }

    @keyframes fade{
      from{opacity:0;transform:translateY(4px)}
      to{opacity:1;transform:translateY(0)}
    }

    .send-row{
      display:flex;
      gap:10px;
      margin-top:12px;
    }

    .send-row .input{flex:1}

    .meter{
      margin-top:14px;
      height:8px;
      background:rgba(255,255,255,.08);
      border-radius:20px;
      overflow:hidden;
    }

    .meter > div{
      width:0%;
      height:100%;
      background:linear-gradient(90deg,var(--primary2),var(--success));
      transition:width .12s;
    }

    .feature-grid{
      display:grid;
      grid-template-columns:1fr 1fr;
      gap:10px;
      margin-top:14px;
    }

    .feature{
      padding:13px;
      border-radius:16px;
      background:rgba(255,255,255,.05);
      border:1px solid rgba(255,255,255,.08);
    }

    .feature b{display:block;margin-bottom:4px}
    .feature span{font-size:12px;color:var(--muted)}

    .footer{
      text-align:center;
      color:#8290b5;
      font-size:12px;
      margin-top:18px;
    }

    audio{display:none}
  </style>
</head>
<body>

<div class="orb one"></div>
<div class="orb two"></div>
<div class="orb three"></div>

<div class="wrap">
  <section class="hero">
    <div class="brand">
      <span class="brand-dot"></span>
      منصة اتصال صوتي خاصة
    </div>

    <h1>سجاد ال علي</h1>
    <p class="subtitle">Voice Connect — اتصال صوتي ودردشة مباشرة بين شخصين</p>
  </section>

  <div class="grid">
    <section class="card">
      <div class="card-head">
        <div class="card-title">🔗 غرفة الاتصال</div>
        <div class="card-sub">أنشئ كود وشاركه مع الطرف الثاني</div>
      </div>

      <div class="card-body">
        <div class="room-box">
          <input class="input" id="room" placeholder="مثال: SAJAD2026">
          <button class="btn btn-primary" onclick="randomRoom()">إنشاء كود</button>
        </div>

        <div class="controls">
          <button class="btn btn-cyan" onclick="connectWS()">الاتصال بالغرفة</button>
          <button class="btn btn-green" onclick="startCall()">بدء المكالمة</button>
          <button class="btn btn-ghost" onclick="toggleMute()" id="muteBtn">كتم المايك</button>
          <button class="btn btn-red" onclick="hangup()">إنهاء المكالمة</button>
        </div>

        <div class="status-card">
          <div class="status-left">
            <span class="pulse" id="pulse"></span>
            <div>
              <div id="status">غير متصل</div>
              <div class="small" id="subStatus">بانتظار اتصال</div>
            </div>
          </div>
          <div class="small" id="roomLabel">لا توجد غرفة</div>
        </div>

        <div class="meter"><div id="meterBar"></div></div>

        <div class="feature-grid">
          <div class="feature">
            <b>🎙️ صوت مباشر</b>
            <span>WebRTC بين الطرفين</span>
          </div>
          <div class="feature">
            <b>💬 دردشة فورية</b>
            <span>رسائل لحظية داخل الغرفة</span>
          </div>
          <div class="feature">
            <b>⚡ سريع</b>
            <span>السيرفر يستخدم للإشارة فقط</span>
          </div>
          <div class="feature">
            <b>🔒 HTTPS</b>
            <span>الاتصال عبر Render</span>
          </div>
        </div>
      </div>
    </section>

    <section class="card">
      <div class="card-head">
        <div class="card-title">💬 المحادثة</div>
        <div class="card-sub">الرسائل تظهر هنا مباشرة</div>
      </div>

      <div class="card-body">
        <div class="log" id="log"></div>

        <div class="send-row">
          <input class="input" id="msg" placeholder="اكتب رسالتك..."
                 onkeydown="if(event.key==='Enter') sendMsg()">
          <button class="btn btn-primary" onclick="sendMsg()">إرسال</button>
        </div>
      </div>
    </section>
  </div>

  <div class="footer">Developed for سجاد ال علي</div>
</div>

<audio id="remoteAudio" autoplay playsinline></audio>

<script>
let ws = null;
let pc = null;
let localStream = null;
let isMuted = false;
let peerReady = false;
let audioCtx = null;
let analyser = null;
let meterTimer = null;

const rtcConfig = {
  iceServers: [
    { urls: "stun:stun.l.google.com:19302" },
    { urls: "stun:stun1.l.google.com:19302" }
  ]
};

function bubble(text, type="system") {
  const log = document.getElementById("log");
  const d = document.createElement("div");
  d.className = "bubble " + type;
  d.textContent = text;
  log.appendChild(d);
  log.scrollTop = log.scrollHeight;
}

function setStatus(main, sub="", online=false) {
  document.getElementById("status").textContent = main;
  document.getElementById("subStatus").textContent = sub;
  document.getElementById("pulse").classList.toggle("online", online);
}

function randomRoom() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let s = "";
  for (let i=0;i<8;i++) s += chars[Math.floor(Math.random()*chars.length)];
  document.getElementById("room").value = s;
  document.getElementById("roomLabel").textContent = "الغرفة: " + s;
  bubble("تم إنشاء كود غرفة جديد: " + s, "system");
}

function sendSignal(payload) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({type:"signal",payload}));
  }
}

async function connectWS() {
  const room = document.getElementById("room").value.trim().toUpperCase();
  if (!room) {
    bubble("أدخل كود الغرفة أولاً.", "system");
    return;
  }

  document.getElementById("roomLabel").textContent = "الغرفة: " + room;

  if (ws && ws.readyState === WebSocket.OPEN) {
    bubble("أنت متصل بالفعل.", "system");
    return;
  }

  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws?room=${encodeURIComponent(room)}`);

  ws.onopen = () => {
    setStatus("متصل بالسيرفر","بانتظار الطرف الثاني",true);
    bubble("تم الاتصال بالسيرفر.", "system");
  };

  ws.onclose = () => {
    setStatus("غير متصل","انقطع الاتصال",false);
    peerReady = false;
    bubble("انقطع الاتصال بالسيرفر.", "system");
  };

  ws.onerror = () => bubble("حدث خطأ بالاتصال.", "system");

  ws.onmessage = async (ev) => {
    let d;
    try { d = JSON.parse(ev.data); } catch { return; }

    if (d.type === "status") {
      bubble(d.message, "system");

      if (d.code === "paired") {
        peerReady = true;
        setStatus("تم ربط الطرفين","جاهز للمكالمة",true);
      }

      if (d.code === "peer_left") {
        peerReady = false;
        setStatus("الطرف الآخر خرج","بانتظار إعادة الاتصال",true);
        cleanupPeerConnection();
      }
    }

    else if (d.type === "message") {
      bubble(d.message, "other");
    }

    else if (d.type === "signal") {
      await handleSignal(d.payload);
    }

    else if (d.type === "error") {
      bubble(d.message, "system");
    }
  };
}

async function ensureLocalAudio() {
  if (localStream) return localStream;

  localStream = await navigator.mediaDevices.getUserMedia({
    audio:true,
    video:false
  });

  bubble("تم تشغيل المايك.", "system");
  startMeter(localStream);
  return localStream;
}

function startMeter(stream) {
  try {
    audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    analyser = audioCtx.createAnalyser();
    analyser.fftSize = 256;

    const src = audioCtx.createMediaStreamSource(stream);
    src.connect(analyser);

    const data = new Uint8Array(analyser.frequencyBinCount);

    clearInterval(meterTimer);
    meterTimer = setInterval(() => {
      analyser.getByteFrequencyData(data);
      let sum = 0;
      for (const v of data) sum += v;
      const avg = sum / data.length;
      const pct = Math.min(100, avg * 1.8);
      document.getElementById("meterBar").style.width = pct + "%";
    }, 120);
  } catch {}
}

async function createPeerConnection() {
  if (pc) return pc;

  pc = new RTCPeerConnection(rtcConfig);

  pc.onicecandidate = (event) => {
    if (event.candidate) {
      sendSignal({kind:"ice",candidate:event.candidate});
    }
  };

  pc.ontrack = (event) => {
    const audio = document.getElementById("remoteAudio");
    audio.srcObject = event.streams[0];
    audio.play().catch(()=>{});
    bubble("تم استقبال صوت الطرف الآخر.", "system");
  };

  pc.onconnectionstatechange = () => {
    const st = pc.connectionState;
    setStatus("حالة المكالمة: " + st, st === "connected" ? "الصوت متصل الآن" : "جاري تحديث الاتصال", st !== "failed");

    if (st === "connected") bubble("تم الاتصال الصوتي بنجاح.", "system");
    if (st === "failed") bubble("فشل الاتصال الصوتي.", "system");
  };

  const stream = await ensureLocalAudio();
  for (const track of stream.getTracks()) {
    pc.addTrack(track, stream);
  }

  return pc;
}

async function startCall() {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    bubble("اتصل بالغرفة أولاً.", "system");
    return;
  }

  if (!peerReady) {
    bubble("انتظر دخول الطرف الثاني.", "system");
    return;
  }

  try {
    await createPeerConnection();
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    sendSignal({kind:"offer",sdp:pc.localDescription});
    setStatus("جاري بدء المكالمة","تم إرسال طلب الاتصال",true);
    bubble("تم إرسال طلب المكالمة.", "system");
  } catch (e) {
    bubble("فشل بدء المكالمة: " + e.message, "system");
  }
}

async function handleSignal(payload) {
  if (!payload || !payload.kind) return;

  try {
    if (payload.kind === "offer") {
      await createPeerConnection();
      await pc.setRemoteDescription(new RTCSessionDescription(payload.sdp));

      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);

      sendSignal({kind:"answer",sdp:pc.localDescription});
      bubble("تم قبول المكالمة.", "system");
    }

    else if (payload.kind === "answer") {
      if (!pc) return;
      await pc.setRemoteDescription(new RTCSessionDescription(payload.sdp));
      bubble("الطرف الآخر قبل المكالمة.", "system");
    }

    else if (payload.kind === "ice") {
      await createPeerConnection();
      if (payload.candidate) {
        try {
          await pc.addIceCandidate(new RTCIceCandidate(payload.candidate));
        } catch {}
      }
    }

    else if (payload.kind === "hangup") {
      bubble("الطرف الآخر أنهى المكالمة.", "system");
      cleanupPeerConnection();
      setStatus("تم إنهاء المكالمة","ما زلت داخل الغرفة",true);
    }
  } catch (e) {
    bubble("خطأ اتصال صوتي: " + e.message, "system");
  }
}

function toggleMute() {
  if (!localStream) {
    bubble("شغّل المكالمة أولاً.", "system");
    return;
  }

  isMuted = !isMuted;

  for (const track of localStream.getAudioTracks()) {
    track.enabled = !isMuted;
  }

  document.getElementById("muteBtn").textContent =
    isMuted ? "تشغيل المايك" : "كتم المايك";

  bubble(isMuted ? "تم كتم المايك." : "تم تشغيل المايك.", "system");
}

function cleanupPeerConnection() {
  if (pc) {
    try { pc.close(); } catch {}
    pc = null;
  }

  document.getElementById("remoteAudio").srcObject = null;
}

function hangup() {
  if (pc) sendSignal({kind:"hangup"});

  cleanupPeerConnection();

  if (localStream) {
    for (const track of localStream.getTracks()) track.stop();
    localStream = null;
  }

  if (meterTimer) clearInterval(meterTimer);
  document.getElementById("meterBar").style.width = "0%";

  isMuted = false;
  document.getElementById("muteBtn").textContent = "كتم المايك";

  setStatus("تم إنهاء المكالمة","ما زلت داخل الغرفة",true);
  bubble("تم إنهاء المكالمة.", "system");
}

function sendMsg() {
  const el = document.getElementById("msg");
  const text = el.value.trim();

  if (!text) return;

  if (!ws || ws.readyState !== WebSocket.OPEN) {
    bubble("مو متصل بالسيرفر.", "system");
    return;
  }

  ws.send(JSON.stringify({
    type:"message",
    message:text
  }));

  bubble(text, "me");
  el.value = "";
}

window.addEventListener("beforeunload", () => {
  try { if (ws) ws.close(); } catch {}
});
</script>
</body>
</html>
"""

async def index(request):
    return web.Response(text=HTML, content_type="text/html")

async def health(request):
    return web.json_response({"ok": True, "rooms": len(rooms)})

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
            "type":"error",
            "message":"الغرفة بيها شخصين بالفعل."
        })
        await ws.close()
        return ws

    members.append(ws)

    if len(members) == 1:
        await ws.send_json({
            "type":"status",
            "code":"waiting",
            "message":f"بانتظار الشخص الثاني. كود الغرفة: {room}"
        })

    elif len(members) == 2:
        for peer in list(members):
            if not peer.closed:
                await peer.send_json({
                    "type":"status",
                    "code":"paired",
                    "message":"تم ربط الطرفين."
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
                    text = str(data.get("message",""))[:5000]
                    if text:
                        await send_to_peer(room, ws, {
                            "type":"message",
                            "message":text
                        })

                elif msg_type == "signal":
                    payload = data.get("payload")
                    if isinstance(payload, dict):
                        await send_to_peer(room, ws, {
                            "type":"signal",
                            "payload":payload
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
                        "type":"status",
                        "code":"peer_left",
                        "message":"الطرف الآخر خرج من الغرفة."
                    })
                except:
                    pass

        if not members:
            rooms.pop(room, None)

    return ws

app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/health", health)
app.router.add_get("/ws", ws_handler)

if __name__ == "__main__":
    port = int(os.environ.get("PORT","10000"))
    web.run_app(app, host="0.0.0.0", port=port)
