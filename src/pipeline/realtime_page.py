from pipeline.realtime_events import FLINK_WINDOWS_WS_PATH, PAGEVIEWS_WS_PATH


def realtime_page() -> str:
    return f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Pipeline Realtime</title>
  <style>
    :root {{
      color-scheme: light dark;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    body {{
      margin: 0;
      background: #f7f7f3;
      color: #202124;
    }}
    main {{
      max-width: 1120px;
      margin: 0 auto;
      padding: 32px 20px;
    }}
    header {{
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 20px;
    }}
    h1 {{
      margin: 0;
      font-size: 28px;
      line-height: 1.2;
      font-weight: 700;
    }}
    button {{
      border: 1px solid #b8b8ad;
      border-radius: 6px;
      background: #ffffff;
      color: #202124;
      cursor: pointer;
      font: inherit;
      padding: 8px 12px;
    }}
    .streams {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 16px;
    }}
    section {{
      min-width: 0;
      border: 1px solid #d4d3c7;
      border-radius: 8px;
      background: #ffffff;
      overflow: hidden;
    }}
    .stream-header {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 14px 16px;
      border-bottom: 1px solid #e4e2d8;
    }}
    h2 {{
      margin: 0;
      font-size: 16px;
      line-height: 1.3;
    }}
    .status {{
      border-radius: 999px;
      background: #ecebe3;
      color: #525047;
      font-size: 12px;
      line-height: 1;
      padding: 6px 8px;
      white-space: nowrap;
    }}
    .status.open {{
      background: #d8f0de;
      color: #1d5f32;
    }}
    .status.closed {{
      background: #f5dddd;
      color: #8a2626;
    }}
    pre {{
      box-sizing: border-box;
      height: 60vh;
      margin: 0;
      overflow: auto;
      padding: 16px;
      background: #151515;
      color: #f1f1ec;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
      font-size: 13px;
      line-height: 1.45;
      white-space: pre-wrap;
      word-break: break-word;
    }}
    @media (max-width: 760px) {{
      header {{
        align-items: flex-start;
        flex-direction: column;
      }}
      .streams {{
        grid-template-columns: 1fr;
      }}
      pre {{
        height: 40vh;
      }}
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <h1>Pipeline Realtime</h1>
      <button id="clear" type="button">Clear</button>
    </header>
    <div class="streams">
      <section>
        <div class="stream-header">
          <h2>Pageviews</h2>
          <span id="pageviews-status" class="status">connecting</span>
        </div>
        <pre id="pageviews-log"></pre>
      </section>
      <section>
        <div class="stream-header">
          <h2>Flink Windows</h2>
          <span id="flink-status" class="status">connecting</span>
        </div>
        <pre id="flink-log"></pre>
      </section>
    </div>
  </main>
  <script>
    const socketProtocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const socketBase = `${{socketProtocol}}//${{window.location.host}}`;

    function append(log, message) {{
      const timestamp = new Date().toISOString();
      try {{
        message = JSON.stringify(JSON.parse(message), null, 2);
      }} catch {{
      }}
      log.textContent += `[${{timestamp}}] ${{message}}\\n\\n`;
      log.scrollTop = log.scrollHeight;
    }}

    function connect(path, statusId, logId) {{
      const status = document.getElementById(statusId);
      const log = document.getElementById(logId);
      const socket = new WebSocket(`${{socketBase}}${{path}}`);

      socket.addEventListener("open", () => {{
        status.textContent = "connected";
        status.className = "status open";
      }});
      socket.addEventListener("message", (event) => append(log, event.data));
      socket.addEventListener("close", () => {{
        status.textContent = "closed";
        status.className = "status closed";
      }});
      socket.addEventListener("error", () => {{
        status.textContent = "error";
        status.className = "status closed";
      }});
    }}

    document.getElementById("clear").addEventListener("click", () => {{
      document.getElementById("pageviews-log").textContent = "";
      document.getElementById("flink-log").textContent = "";
    }});

    connect("{PAGEVIEWS_WS_PATH}", "pageviews-status", "pageviews-log");
    connect("{FLINK_WINDOWS_WS_PATH}", "flink-status", "flink-log");
  </script>
</body>
</html>
"""
