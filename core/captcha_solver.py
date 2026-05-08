from __future__ import annotations

import json
import socket
from typing import Any


default_host = 'captcha.hawker.news'
default_time_out = 180


def connect_client(host_used, server_time_out):
    host = host_used
    port = 4444
    socket_client = socket.socket()
    socket_client.connect((host, port))
    socket_client.settimeout(server_time_out)
    return socket_client


def captcha_to_text(source, host=default_host, default_captcha_source='model_captcha', time_out=default_time_out):
    temp_dict_source = dict()
    temp_dict_source['image_source'] = source
    temp_dict_source['captcha_source'] = default_captcha_source
    client = connect_client(host, time_out)
    send_data(client, temp_dict_source)
    response = receive_data(client)
    client.close()
    return response['image_text']


def send_data(client, data):
    try:
        serialized = json.dumps(data)
    except (TypeError, ValueError) as e:
        raise Exception('You can only send JSON-serializable data')
    client.send(b'%d\n' % len(serialized))
    client.sendall(serialized.encode())


def receive_data(client):
    length_str = ''
    char = client.recv(1)
    while char != b'\n':
        length_str += char.decode()
        char = client.recv(1)
        if not char:
            raise Exception('DAta Received is Empty Bytes')
    total = int(length_str)
    view = memoryview(bytearray(total))
    next_offset = 0
    while total - next_offset > 0:
        recv_size = client.recv_into(view[next_offset:], total - next_offset)
        next_offset += recv_size
    try:
        deserialized = json.loads(view.tobytes())
    except (TypeError, ValueError) as e:
        raise Exception('Data received was not in JSON format')
    return deserialized


def _logger_call(logger: Any, level: str, message: str) -> None:
    if logger is None:
        return
    log_fn = getattr(logger, level, None)
    if callable(log_fn):
        try:
            log_fn(message, step='captcha')
        except TypeError:
            log_fn(message)


def extract_captcha_source_from_page(page, selectors: list[str] | None = None) -> str | None:
    """
    Return a JSON-serializable image source from a Playwright page.

    Preference order:
    1. Explicit selectors passed by the caller.
    2. Elements whose attributes suggest they are captcha widgets.
    3. Visible canvas/img elements as a final fallback.
    """
    selector_candidates = selectors or []
    return page.evaluate(
        """
        (selectors) => {
            const toSource = (el) => {
                if (!el) return null;
                const tag = (el.tagName || "").toLowerCase();
                try {
                    if (tag === "canvas" && typeof el.toDataURL === "function") {
                        const dataUrl = el.toDataURL("image/png");
                        return dataUrl && dataUrl !== "data:," ? dataUrl : null;
                    }
                    if (tag === "img") {
                        return el.currentSrc || el.src || null;
                    }
                } catch (err) {
                    return null;
                }
                return null;
            };

            for (const selector of selectors || []) {
                const source = toSource(document.querySelector(selector));
                if (source) return source;
            }

            const candidates = Array.from(document.querySelectorAll("canvas, img"))
                .map((el) => {
                    const attrs = [
                        el.id,
                        el.getAttribute("name"),
                        el.className,
                        el.getAttribute("alt"),
                        el.getAttribute("aria-label"),
                        el.getAttribute("src"),
                    ].filter(Boolean).join(" ");
                    const rect = typeof el.getBoundingClientRect === "function"
                        ? el.getBoundingClientRect()
                        : { width: 0, height: 0 };
                    return {
                        el,
                        source: toSource(el),
                        hint: /captcha/i.test(attrs),
                        area: (rect.width || 0) * (rect.height || 0),
                    };
                })
                .filter((item) => item.source);

            candidates.sort((a, b) => {
                if (a.hint !== b.hint) return Number(b.hint) - Number(a.hint);
                return b.area - a.area;
            });

            return candidates.length ? candidates[0].source : null;
        }
        """,
        selector_candidates,
    )


def wait_for_captcha_canvas(
    page,
    selector: str = "canvas",
    *,
    timeout_ms: int = 20_000,
    poll_interval_ms: int = 500,
    logger: Any = None,
) -> bool:
    """
    Block until a canvas element is visible AND has non-blank pixel data.

    Returns True if the canvas is ready within *timeout_ms*, False otherwise.
    Uses Playwright's wait_for_function so we poll inside the browser rather
    than sleeping on the Python side.
    """
    try:
        page.wait_for_selector(selector, state="visible", timeout=timeout_ms)
    except Exception as exc:
        _logger_call(logger, "warning", f"Canvas selector '{selector}' not found: {exc}")
        return False

    js = f"""
    () => {{
        const canvas = document.querySelector({json.dumps(selector)});
        if (!canvas || typeof canvas.toDataURL !== 'function') return false;
        try {{
            const dataUrl = canvas.toDataURL('image/png');
            if (!dataUrl || dataUrl === 'data:,') return false;
            const ctx = canvas.getContext('2d');
            if (!ctx) return dataUrl.length > 200;
            const d = ctx.getImageData(0, 0, canvas.width, canvas.height).data;
            for (let i = 0; i < d.length; i += 4) {{
                if (d[i] < 250 || d[i+1] < 250 || d[i+2] < 250) return true;
            }}
            return false;
        }} catch (e) {{
            return false;
        }}
    }}
    """
    try:
        page.wait_for_function(js, timeout=timeout_ms, polling=poll_interval_ms)
        _logger_call(logger, "info", "Canvas captcha is rendered and ready")
        return True
    except Exception as exc:
        _logger_call(logger, "warning", f"Canvas not ready within timeout: {exc}")
        return False


def solve_captcha_from_page(
    page,
    *,
    logger: Any = None,
    selectors: list[str] | None = None,
    captcha_source: str = 'model_captcha',
    host: str = default_host,
    time_out: int = default_time_out,
) -> str | None:
    """Extract a captcha image from a Playwright page and send it to the solver."""
    try:
        image_source = extract_captcha_source_from_page(page, selectors=selectors)
        if not image_source:
            _logger_call(logger, 'warning', 'Captcha image source not found on page')
            return None

        solved_text = captcha_to_text(
            image_source,
            host=host,
            default_captcha_source=captcha_source,
            time_out=time_out,
        )
        solved_text = (solved_text or '').strip()
        if solved_text:
            return solved_text
        _logger_call(logger, 'warning', 'Captcha solver returned empty text')
        return None
    except Exception as exc:
        _logger_call(logger, 'warning', f'Captcha solver failed: {exc}')
        return None
