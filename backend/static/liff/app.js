const LIFF_ID = "2009772212-G8H9NKjo";
const API_BASE = "";

let currentUserId = null;
let currentDisplayName = null;
let currentGroupToken = null;

const statusEl = document.getElementById("status");
const profileEl = document.getElementById("profile");
const groupInfoEl = document.getElementById("groupInfo");
const monthInput = document.getElementById("monthInput");
const lunchDaysEl = document.getElementById("lunchDays");
const dinnerDaysEl = document.getElementById("dinnerDays");
const noteInput = document.getElementById("noteInput");
const saveButton = document.getElementById("saveButton");
const boardEl = document.getElementById("board");

function setStatus(message, type = "normal") {
  const className =
    type === "success"
      ? "status-box status-success"
      : type === "error"
      ? "status-box status-error"
      : "status-box status-normal";

  statusEl.innerHTML = `<div class="${className}">${message}</div>`;
}

function getTodayMonth() {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  return `${year}-${month}`;
}

function getDaysInMonth(targetMonth) {
  const [year, month] = targetMonth.split("-").map(Number);
  return new Date(year, month, 0).getDate();
}

function getWeekdayLabel(targetMonth, day) {
  const [year, month] = targetMonth.split("-").map(Number);
  const date = new Date(year, month - 1, day);
  const labels = ["日", "月", "火", "水", "木", "金", "土"];
  return labels[date.getDay()];
}

function getGroupTokenFromUrl() {
  const params = new URLSearchParams(window.location.search);
  return params.get("group_token");
}

function getSelectedDays(container) {
  const checked = [...container.querySelectorAll("input[type='checkbox']:checked")];
  return checked.map((input) => Number(input.value)).sort((a, b) => a - b);
}

function renderDayCheckboxes(container, targetMonth, selectedDays = []) {
  container.innerHTML = "";
  const daysInMonth = getDaysInMonth(targetMonth);

  for (let day = 1; day <= daysInMonth; day++) {
    const weekday = getWeekdayLabel(targetMonth, day);
    const wrapper = document.createElement("label");
    wrapper.className = "day-item";

    if (weekday === "土") wrapper.classList.add("sat");
    if (weekday === "日") wrapper.classList.add("sun");

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.value = String(day);
    checkbox.checked = selectedDays.includes(day);

    const text = document.createElement("span");
    text.textContent = `${day}日 (${weekday})`;

    wrapper.appendChild(checkbox);
    wrapper.appendChild(text);
    container.appendChild(wrapper);
  }
}

function buildCountRow(members, shiftMap, daysInMonth) {
  const counts = {};
  for (let day = 1; day <= daysInMonth; day++) counts[day] = 0;

  members.forEach((member) => {
    const userId = member.line_user_id;
    const days = shiftMap[userId] || [];
    days.forEach((day) => {
      counts[day] += 1;
    });
  });

  return counts;
}

function buildLegendHtml() {
  return `
    <div class="legend">
      <div class="legend-item"><span class="legend-box legend-self"></span>自分の行</div>
      <div class="legend-item"><span class="legend-box legend-sat"></span>土曜日</div>
      <div class="legend-item"><span class="legend-box legend-sun"></span>日曜日</div>
      <div class="legend-item"><span class="legend-box legend-count"></span>人数行</div>
    </div>
  `;
}

function renderBoardSection(title, targetMonth, members, shiftMap) {
  const daysInMonth = getDaysInMonth(targetMonth);
  const counts = buildCountRow(members, shiftMap, daysInMonth);

  let html = `
    <div class="board-section">
      <h3>${title}</h3>
      ${buildLegendHtml()}
      <div class="board-table-wrapper">
        <table class="board-table">
          <thead>
            <tr>
              <th>名前</th>
  `;

  for (let day = 1; day <= daysInMonth; day++) {
    const weekday = getWeekdayLabel(targetMonth, day);
    let className = "";
    if (weekday === "土") className = "weekday-sat";
    if (weekday === "日") className = "weekday-sun";

    html += `<th class="${className}">${day}<br>${weekday}</th>`;
  }

  html += `
            </tr>
          </thead>
          <tbody>
  `;

  members.forEach((member) => {
    const isSelf = member.line_user_id === currentUserId;
    const rowClass = isSelf ? "self-row" : "";
    const displayName = member.display_name || member.line_user_id;
    const days = shiftMap[member.line_user_id] || [];

    html += `<tr class="${rowClass}"><td>${displayName}${isSelf ? " ◀ 自分" : ""}</td>`;

    for (let day = 1; day <= daysInMonth; day++) {
      html += `<td>${days.includes(day) ? "○" : ""}</td>`;
    }

    html += `</tr>`;
  });

  html += `<tr class="count-row"><td>人数</td>`;
  for (let day = 1; day <= daysInMonth; day++) {
    html += `<td>${counts[day]}</td>`;
  }
  html += `</tr>`;

  html += `
          </tbody>
        </table>
      </div>
    </div>
  `;

  return html;
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `HTTP ${response.status}`);
  }
  return response.json();
}

async function loadBoard() {
  const month = monthInput.value;
  const boardUrl = `${API_BASE}/api/liff/groups/${currentGroupToken}/shift-board?month=${month}`;
  const myShiftUrl = `${API_BASE}/api/liff/groups/${currentGroupToken}/my-shift?month=${month}&line_user_id=${encodeURIComponent(currentUserId)}`;

  setStatus("読み込み中...", "normal");

  const [boardData, myShift] = await Promise.all([
    fetchJson(boardUrl),
    fetchJson(myShiftUrl),
  ]);

  groupInfoEl.textContent = `group_token: ${currentGroupToken}`;
  noteInput.value = myShift.note || "";

  renderDayCheckboxes(lunchDaysEl, month, myShift.lunch_days || []);
  renderDayCheckboxes(dinnerDaysEl, month, myShift.dinner_days || []);

  const lunchHtml = renderBoardSection("LUNCH", month, boardData.members, boardData.lunch);
  const dinnerHtml = renderBoardSection("DINNER", month, boardData.members, boardData.dinner);
  boardEl.innerHTML = lunchHtml + dinnerHtml;

  setStatus("読み込み完了", "success");
}

async function saveShift() {
  try {
    saveButton.disabled = true;
    setStatus("保存中...", "normal");

    const payload = {
      line_user_id: currentUserId,
      target_month: monthInput.value,
      lunch_days: getSelectedDays(lunchDaysEl),
      dinner_days: getSelectedDays(dinnerDaysEl),
      note: noteInput.value,
    };

    await fetchJson(
      `${API_BASE}/api/liff/groups/${currentGroupToken}/shift-board`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      }
    );

    await loadBoard();
    setStatus("保存しました。全体表を更新しました。", "success");
  } catch (error) {
    console.error(error);
    setStatus(`保存に失敗しました: ${error.message}`, "error");
  } finally {
    saveButton.disabled = false;
  }
}

async function initApp() {
  try {
    currentGroupToken = getGroupTokenFromUrl();
    if (!currentGroupToken) {
      setStatus("group_token がURLにありません", "error");
      return;
    }

    monthInput.value = getTodayMonth();

    await liff.init({ liffId: LIFF_ID });

    if (!liff.isLoggedIn()) {
      liff.login();
      return;
    }

    const profile = await liff.getProfile();
    currentUserId = profile.userId;
    currentDisplayName = profile.displayName;

    setStatus("LIFF初期化成功", "success");
    profileEl.textContent = `ログイン中: ${currentDisplayName} (${currentUserId})`;

    await loadBoard();
  } catch (error) {
    console.error(error);
    setStatus(`初期化エラー: ${error.message}`, "error");
  }
}

monthInput.addEventListener("change", loadBoard);
saveButton.addEventListener("click", saveShift);

initApp();