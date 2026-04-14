const ADMIN_LIFF_ID = "2009772212-JhnIEnYt";
const API_BASE = "";

let currentUserId = null;
let currentDisplayName = null;
let currentGroupToken = null;
let currentGroups = [];
let currentMembers = [];
let currentStatus = null;

const statusEl = document.getElementById("status");
const profileEl = document.getElementById("profile");
const groupInfoEl = document.getElementById("groupInfo");
const monthInput = document.getElementById("monthInput");
const groupSelect = document.getElementById("groupSelect");
const syncMembersButton = document.getElementById("syncMembersButton");
const homeSummaryEl = document.getElementById("homeSummary");
const targetsListEl = document.getElementById("targetsList");
const requirementsTableWrapperEl = document.getElementById("requirementsTableWrapper");
const statusSummaryEl = document.getElementById("statusSummary");
const missingMembersEl = document.getElementById("missingMembers");
const submittedMembersEl = document.getElementById("submittedMembers");
const dailyStatusTableWrapperEl = document.getElementById("dailyStatusTableWrapper");
const botResultEl = document.getElementById("botResult");

const saveTargetsButton = document.getElementById("saveTargetsButton");
const saveRequirementsButton = document.getElementById("saveRequirementsButton");
const selectAllTargetsButton = document.getElementById("selectAllTargets");
const clearAllTargetsButton = document.getElementById("clearAllTargets");

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

function getInitialGroupTokenFromUrl() {
  const params = new URLSearchParams(window.location.search);
  return params.get("group_token");
}

function getDaysInMonth(targetMonth) {
  const [year, month] = targetMonth.split("-").map(Number);
  return new Date(year, month, 0).getDate();
}

function fetchJson(url, options = {}) {
  return fetch(url, options).then(async (response) => {
    if (!response.ok) {
      const text = await response.text();
      throw new Error(text || `HTTP ${response.status}`);
    }
    return response.json();
  });
}

function activateTab(tabName) {
  document.querySelectorAll(".tab-button").forEach((button) => {
    button.classList.toggle("active", button.dataset.tab === tabName);
  });

  document.querySelectorAll(".tab-panel").forEach((panel) => {
    panel.classList.toggle("active", panel.id === `tab-${tabName}`);
  });
}

function renderGroupSelect(groups, initialToken) {
  groupSelect.innerHTML = "";

  groups.forEach((group) => {
    const option = document.createElement("option");
    option.value = group.group_token;
    option.textContent = `${group.group_name} (${group.member_count}人)`;
    groupSelect.appendChild(option);
  });

  if (initialToken && groups.some((g) => g.group_token === initialToken)) {
    groupSelect.value = initialToken;
  } else if (groups.length > 0) {
    groupSelect.value = groups[0].group_token;
  }

  currentGroupToken = groupSelect.value;
}

function updateGroupInfo() {
  const selected = currentGroups.find((g) => g.group_token === currentGroupToken);
  if (!selected) {
    groupInfoEl.textContent = "";
    return;
  }
  groupInfoEl.textContent = `対象グループ: ${selected.group_name} / group_token: ${selected.group_token}`;
}

function renderHomeSummary(status) {
  homeSummaryEl.innerHTML = `
    <div class="summary-card">
      <div class="summary-label">対象人数</div>
      <div class="summary-value">${status.target_count}</div>
    </div>
    <div class="summary-card">
      <div class="summary-label">提出済み</div>
      <div class="summary-value">${status.submitted_count}</div>
    </div>
    <div class="summary-card">
      <div class="summary-label">未提出</div>
      <div class="summary-value">${status.missing_count}</div>
    </div>
    <div class="summary-card">
      <div class="summary-label">対象月</div>
      <div class="summary-value">${status.target_month}</div>
    </div>
  `;
}

function renderTargets(members, targetIds) {
  targetsListEl.innerHTML = "";
  members.forEach((member) => {
    const label = document.createElement("label");
    label.className = "check-item";

    const input = document.createElement("input");
    input.type = "checkbox";
    input.value = member.line_user_id;
    input.checked = targetIds.includes(member.line_user_id);

    const text = document.createElement("span");
    text.textContent = member.display_name || member.line_user_id;

    label.appendChild(input);
    label.appendChild(text);
    targetsListEl.appendChild(label);
  });
}

function renderRequirements(targetMonth, requirements) {
  const daysInMonth = getDaysInMonth(targetMonth);
  const requirementMap = {};
  requirements.forEach((req) => {
    requirementMap[req.shift_date] = req;
  });

  let html = `
    <table class="admin-table">
      <thead>
        <tr>
          <th>日付</th>
          <th>定休日</th>
          <th>LUNCH必要人数</th>
          <th>DINNER必要人数</th>
          <th>備考</th>
        </tr>
      </thead>
      <tbody>
  `;

  for (let day = 1; day <= daysInMonth; day++) {
    const shiftDate = `${targetMonth}-${String(day).padStart(2, "0")}`;
    const req = requirementMap[shiftDate] || {
      shift_date: shiftDate,
      is_closed: false,
      lunch_required: 0,
      dinner_required: 0,
      note: "",
    };

    html += `
      <tr>
        <td>${shiftDate}</td>
        <td><input type="checkbox" data-type="is_closed" data-date="${shiftDate}" ${req.is_closed ? "checked" : ""}></td>
        <td><input type="number" min="0" value="${req.lunch_required}" data-type="lunch_required" data-date="${shiftDate}"></td>
        <td><input type="number" min="0" value="${req.dinner_required}" data-type="dinner_required" data-date="${shiftDate}"></td>
        <td><input type="text" value="${req.note || ""}" data-type="note" data-date="${shiftDate}"></td>
      </tr>
    `;
  }

  html += `</tbody></table>`;
  requirementsTableWrapperEl.innerHTML = html;
}

function renderMemberList(container, members) {
  container.innerHTML = "";
  if (!members.length) {
    container.innerHTML = `<div class="simple-list-item">なし</div>`;
    return;
  }

  members.forEach((member) => {
    const div = document.createElement("div");
    div.className = "simple-list-item";
    div.textContent = member.display_name;
    container.appendChild(div);
  });
}

function renderStatus(status) {
  statusSummaryEl.innerHTML = `
    <div class="summary-card">
      <div class="summary-label">対象人数</div>
      <div class="summary-value">${status.target_count}</div>
    </div>
    <div class="summary-card">
      <div class="summary-label">提出済み</div>
      <div class="summary-value">${status.submitted_count}</div>
    </div>
    <div class="summary-card">
      <div class="summary-label">未提出</div>
      <div class="summary-value">${status.missing_count}</div>
    </div>
  `;

  renderMemberList(missingMembersEl, status.missing_members);
  renderMemberList(submittedMembersEl, status.submitted_members);

  let html = `
    <table class="admin-table">
      <thead>
        <tr>
          <th>日付</th>
          <th>定休日</th>
          <th>L必要</th>
          <th>L提出</th>
          <th>L差分</th>
          <th>D必要</th>
          <th>D提出</th>
          <th>D差分</th>
          <th>備考</th>
        </tr>
      </thead>
      <tbody>
  `;

  status.daily_status.forEach((row) => {
    html += `
      <tr>
        <td>${row.shift_date}</td>
        <td>${row.is_closed ? "○" : ""}</td>
        <td>${row.lunch_required}</td>
        <td>${row.lunch_submitted}</td>
        <td>${row.lunch_diff}</td>
        <td>${row.dinner_required}</td>
        <td>${row.dinner_submitted}</td>
        <td>${row.dinner_diff}</td>
        <td>${row.note || ""}</td>
      </tr>
    `;
  });

  html += `</tbody></table>`;
  dailyStatusTableWrapperEl.innerHTML = html;
}

function getSelectedTargetIds() {
  return [...targetsListEl.querySelectorAll("input[type='checkbox']:checked")].map((input) => input.value);
}

function getRequirementPayload() {
  const daysInMonth = getDaysInMonth(monthInput.value);
  const requirements = [];

  for (let day = 1; day <= daysInMonth; day++) {
    const shiftDate = `${monthInput.value}-${String(day).padStart(2, "0")}`;
    const isClosed = requirementsTableWrapperEl.querySelector(`[data-type="is_closed"][data-date="${shiftDate}"]`)?.checked ?? false;
    const lunchRequired = Number(requirementsTableWrapperEl.querySelector(`[data-type="lunch_required"][data-date="${shiftDate}"]`)?.value ?? 0);
    const dinnerRequired = Number(requirementsTableWrapperEl.querySelector(`[data-type="dinner_required"][data-date="${shiftDate}"]`)?.value ?? 0);
    const note = requirementsTableWrapperEl.querySelector(`[data-type="note"][data-date="${shiftDate}"]`)?.value ?? "";

    requirements.push({
      shift_date: shiftDate,
      is_closed: isClosed,
      lunch_required: lunchRequired,
      dinner_required: dinnerRequired,
      note: note,
    });
  }

  return requirements;
}

async function loadGroups() {
  const data = await fetchJson(`${API_BASE}/api/admin/groups`);
  currentGroups = data.groups || [];
  renderGroupSelect(currentGroups, getInitialGroupTokenFromUrl());
  updateGroupInfo();
}

async function loadAll() {
  if (!currentGroupToken) {
    setStatus("対象グループがありません", "error");
    return;
  }

  const month = monthInput.value;
  setStatus("読み込み中...", "normal");

  const [membersData, targets, requirements, status] = await Promise.all([
    fetchJson(`${API_BASE}/api/admin/groups/${currentGroupToken}/members`),
    fetchJson(`${API_BASE}/api/admin/groups/${currentGroupToken}/targets?month=${month}`),
    fetchJson(`${API_BASE}/api/admin/groups/${currentGroupToken}/requirements?month=${month}`),
    fetchJson(`${API_BASE}/api/admin/groups/${currentGroupToken}/status?month=${month}`),
  ]);

  currentMembers = membersData.members;
  currentStatus = status;

  renderHomeSummary(status);
  renderTargets(membersData.members, targets.line_user_ids);
  renderRequirements(month, requirements.requirements);
  renderStatus(status);
  updateGroupInfo();

  setStatus("読み込み完了", "success");
}

async function syncMembers() {
  try {
    setStatus("メンバー同期中...", "normal");
    const result = await fetchJson(`${API_BASE}/api/admin/groups/${currentGroupToken}/sync-members`, {
      method: "POST",
    });
    await loadGroups();
    await loadAll();
    setStatus(`メンバー同期完了: ${result.synced_count}件`, "success");
  } catch (error) {
    console.error(error);
    setStatus(`メンバー同期に失敗しました: ${error.message}`, "error");
  }
}

async function saveTargets() {
  try {
    setStatus("対象者を保存中...", "normal");

    await fetchJson(`${API_BASE}/api/admin/groups/${currentGroupToken}/targets`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        target_month: monthInput.value,
        line_user_ids: getSelectedTargetIds(),
      }),
    });

    await loadAll();
    setStatus("対象者を保存しました", "success");
  } catch (error) {
    console.error(error);
    setStatus(`対象者保存に失敗しました: ${error.message}`, "error");
  }
}

async function saveRequirements() {
  try {
    setStatus("営業条件を保存中...", "normal");

    await fetchJson(`${API_BASE}/api/admin/groups/${currentGroupToken}/requirements`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        target_month: monthInput.value,
        requirements: getRequirementPayload(),
      }),
    });

    await loadAll();
    setStatus("営業条件を保存しました", "success");
  } catch (error) {
    console.error(error);
    setStatus(`営業条件保存に失敗しました: ${error.message}`, "error");
  }
}

async function runBotJob(jobType, label) {
  try {
    setStatus(`${label}を送信中...`, "normal");

    const result = await fetchJson(`${API_BASE}/api/admin/groups/${currentGroupToken}/bot/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        job_type: jobType,
        target_month: monthInput.value,
      }),
    });

    botResultEl.textContent = result.message_text;
    setStatus(`${label}を送信しました`, "success");
  } catch (error) {
    console.error(error);
    botResultEl.textContent = "";
    setStatus(`${label}に失敗しました: ${error.message}`, "error");
  }
}

function setupBotButtons() {
  document.getElementById("runRecruitButton").addEventListener("click", () => {
    runBotJob("recruit", "募集メッセージ");
  });

  document.getElementById("runMissingButton").addEventListener("click", () => {
    runBotJob("missing_reminder", "未提出通知");
  });

  document.getElementById("runSummaryButton").addEventListener("click", () => {
    runBotJob("status_summary", "提出状況通知");
  });

  document.getElementById("runShortageButton").addEventListener("click", () => {
    runBotJob("shortage_summary", "不足日通知");
  });
}

function setupTabs() {
  document.querySelectorAll(".tab-button").forEach((button) => {
    button.addEventListener("click", () => {
      activateTab(button.dataset.tab);
    });
  });
}

function setupTargetButtons() {
  selectAllTargetsButton.addEventListener("click", () => {
    targetsListEl.querySelectorAll("input[type='checkbox']").forEach((input) => {
      input.checked = true;
    });
  });

  clearAllTargetsButton.addEventListener("click", () => {
    targetsListEl.querySelectorAll("input[type='checkbox']").forEach((input) => {
      input.checked = false;
    });
  });
}

async function initApp() {
  try {
    monthInput.value = getTodayMonth();

    await liff.init({ liffId: ADMIN_LIFF_ID });

    if (!liff.isLoggedIn()) {
      liff.login();
      return;
    }

    const profile = await liff.getProfile();
    currentUserId = profile.userId;
    currentDisplayName = profile.displayName;

    profileEl.textContent = `ログイン中: ${currentDisplayName} (${currentUserId})`;

    setupTabs();
    setupBotButtons();
    setupTargetButtons();

    saveTargetsButton.addEventListener("click", saveTargets);
    saveRequirementsButton.addEventListener("click", saveRequirements);
    syncMembersButton.addEventListener("click", syncMembers);

    groupSelect.addEventListener("change", async () => {
      currentGroupToken = groupSelect.value;
      await loadAll();
    });

    monthInput.addEventListener("change", loadAll);

    await loadGroups();
    await loadAll();
  } catch (error) {
    console.error(error);
    setStatus(`初期化エラー: ${error.message}`, "error");
  }
}

initApp();