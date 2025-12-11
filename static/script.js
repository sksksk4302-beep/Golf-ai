// ✅ 전체 수정된 script.js
const hourCheckboxes = document.querySelectorAll(".hour-checkbox");
const startDateInput = document.getElementById("start_date");
const endDateInput = document.getElementById("end_date");
const resultBody = document.getElementById("result-body");
const settingsModal = document.getElementById("settings-modal");
const golfclubList = document.getElementById("golfclub-list");
const priceCheckboxes = document.querySelectorAll(".price-checkbox");

let currentRegion = "경기";
let golfclubData = [];
let golfclubMeta = [];
let currentFavorites = {}; // region별 선택 상태 유지

window.onload = () => {
  const tomorrow = new Date();
  tomorrow.setDate(tomorrow.getDate() + 1);
  const formatted = tomorrow.toISOString().split("T")[0];
  startDateInput.value = formatted;
  endDateInput.value = formatted;

  // ✅ golfclubMeta 초기 로드 후 데이터 준비
  fetch("/static/golf_clubs.json")
    .then(r => r.json())
    .then(d => {
      golfclubMeta = d;
      loadAllGolfclubs();
    });

  // ✅ 1시간마다 자동으로 /admin/refresh 호출
  triggerAutoRefresh(); // 최초 1회
  setInterval(triggerAutoRefresh, 60 * 60 * 1000); // 매 1시간마다
};

function triggerAutoRefresh() {
  fetch("/admin/refresh", { method: "POST" })
    .then(() => console.log("🔁 /admin/refresh 자동 호출 완료"))
    .catch(err => console.error("❌ 자동 갱신 실패:", err));
}

function getTimeNumber(hourStr) {
  const match = hourStr.match(/(\d{1,2})시대/);
  return match ? parseInt(match[1], 10) : 0;
}

function getSortedKeys(grouped) {
  return Object.keys(grouped).sort((a, b) => {
    const [dateA, hourA] = a.split(" ");
    const [dateB, hourB] = b.split(" ");
    const fullDateA = new Date(`2025-${dateA.replace("/", "-")}`);
    const fullDateB = new Date(`2025-${dateB.replace("/", "-")}`);
    if (fullDateA.getTime() !== fullDateB.getTime()) {
      return fullDateA - fullDateB;
    }
    return getTimeNumber(hourA) - getTimeNumber(hourB);
  });
}

function formatToManWon(price) {
  return `${(price / 10000).toFixed(1)}`;
}

function getPriceFilterRange() {
  const checked = Array.from(priceCheckboxes).filter(cb => cb.checked).map(cb => cb.value);
  return function (price) {
    if (checked.includes("10") && price <= 100000) return true;
    if (checked.includes("15") && price <= 150000) return true;
    if (checked.includes("over") && price > 150000) return true;
    return checked.length === 0;
  };
}

async function getGroupedTeeTime() {
  const start_date = startDateInput.value;
  const end_date = endDateInput.value;
  const checkedHours = Array.from(hourCheckboxes).filter(cb => cb.checked).map(cb => parseInt(cb.value));
  const favoriteClubs = getFavoriteClubs();
  const priceFilter = getPriceFilterRange();

  console.log("📤 티타임 요청 시작", { start_date, end_date, checkedHours, favoriteClubs });
  resultBody.innerHTML = `<tr><td colspan="100%">⏳ 조회 중...</td></tr>`;

  try {
    const response = await fetch("/get_ttime_grouped", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ start_date, end_date, hour_range: checkedHours.length ? checkedHours : null, favorite_clubs: favoriteClubs })
    });

    const data = (await response.json()).filter(item => priceFilter(item.price));
    console.log("✅ 티타임 응답 도착", data);
    renderTeeTimeTable(data);
  } catch (err) {
    console.error("❌ 요청 실패 또는 서버 오류:", err);
    resultBody.innerHTML = `<tr><td colspan="100%">요청 실패 또는 서버 오류</td></tr>`;
  }
}

function renderTeeTimeTable(data) {
  const grouped = {};
  const golfNames = new Set();

  for (const item of data) {
    const key = `${item.date} ${item.hour}`;
    if (!grouped[key]) grouped[key] = {};
    if (!grouped[key][item.golf] || grouped[key][item.golf].source === "golfpang") {
      grouped[key][item.golf] = item;
    }
    golfNames.add(item.golf);
  }

  const sortedGolfNames = Array.from(golfNames).sort();
  const sortedKeys = getSortedKeys(grouped);

  const thead = document.querySelector("thead tr");
  thead.innerHTML = `<th>날짜/시간대</th>` + sortedGolfNames.map(name => `<th title="${name}">${name}</th>`).join("");
  resultBody.innerHTML = "";

  let lastDate = null;
  for (const key of sortedKeys) {
    const [date] = key.split(" ");
    const priceMap = grouped[key];
    if (!Object.keys(priceMap).length) continue;

    const tr = document.createElement("tr");
    if (date !== lastDate) {
      tr.classList.add("new-date");
      lastDate = date;
    }
    const tdLabel = document.createElement("td");
    tdLabel.textContent = key;
    tr.appendChild(tdLabel);

    let minPrice = Infinity;
    Object.values(priceMap).forEach(p => { if (p.price < minPrice) minPrice = p.price });

    sortedGolfNames.forEach(name => {
      const td = document.createElement("td");
      const item = priceMap[name];
      if (item) {
        if (item.price === minPrice) td.classList.add("highlight");
        const iconColor = item.source === "teescan" ? "red" : "blue";
        const icon = `<span style="display:inline-block;width:14px;height:14px;border-radius:50%;background:${iconColor};color:white;font-size:10px;line-height:14px;text-align:center;margin-right:3px;font-weight:bold;">${item.source === "teescan" ? "T" : "G"}</span>`;
        td.innerHTML = `<div class="price-cell" data-url="${item.url}" style="cursor:pointer;">${icon}${formatToManWon(item.price)}</div>`;
      } else {
        td.textContent = "-";
      }
      tr.appendChild(td);
    });
    resultBody.appendChild(tr);
  }
}

function getRegionByAddress(addr) {
  if (addr.startsWith("경기도")) return "경기";
  if (addr.startsWith("충청")) return "충청";
  if (addr.startsWith("강원")) return "강원";
  return "기타";
}

function loadAllGolfclubs() {
  fetch("/get_all_golfclubs")
    .then(res => res.json())
    .then(clubs => {
      golfclubData = clubs.map(name => {
        const matched = golfclubMeta.find(c => c.name === name);
        return { name, region: matched ? getRegionByAddress(matched.address || "") : "기타" };
      });
      // 모든 지역의 체크 상태를 한 번에 불러오기
      const favs = getFavoriteClubs();
      ["경기", "충청", "강원", "기타"].forEach(region => {
        currentFavorites[region] = golfclubData.filter(c => c.region === region && favs.includes(c.name)).map(c => c.name);
      });
      renderGolfclubList(currentRegion);
    });
}

function renderGolfclubList(region) {
  currentRegion = region;
  golfclubList.innerHTML = "";

  const regionFavorites = currentFavorites[region] || [];

  golfclubData.filter(c => c.region === region).forEach(({ name }) => {
    const checked = regionFavorites.includes(name) ? "checked" : "";
    golfclubList.innerHTML += `<label><input type="checkbox" value="${name}" ${checked}> ${name}</label>`;
  });

  document.querySelectorAll(".tab-button").forEach(btn => {
    btn.classList.toggle("active", btn.dataset.region === region);
  });
}

function selectAllCurrentRegion(select) {
  const checkboxes = golfclubList.querySelectorAll("input[type='checkbox']");
  checkboxes.forEach(cb => { cb.checked = select });
  currentFavorites[currentRegion] = select ? golfclubData.filter(c => c.region === currentRegion).map(c => c.name) : [];
}

function saveFavorites() {
  const checkboxes = document.querySelectorAll("#golfclub-list input[type='checkbox']");
  const selected = Array.from(checkboxes).filter(cb => cb.checked).map(cb => cb.value);
  currentFavorites[currentRegion] = selected;

  const allSelected = Object.values(currentFavorites).flat();
  localStorage.setItem("favorite_clubs", JSON.stringify(allSelected));
  console.log("💾 선호 골프장 저장됨:", allSelected);
  closeModal();
  getGroupedTeeTime();
}

function openModal() {
  settingsModal.style.display = "block";
}

function closeModal() {
  settingsModal.style.display = "none";
}

document.getElementById("settings-button").addEventListener("click", openModal);
document.querySelectorAll(".tab-button").forEach(btn => {
  btn.addEventListener("click", () => renderGolfclubList(btn.dataset.region));
});

function getFavoriteClubs() {
  try {
    return JSON.parse(localStorage.getItem("favorite_clubs") || "[]");
  } catch {
    return [];
  }
}

resultBody.addEventListener("click", e => {
  const target = e.target.closest(".price-cell");
  if (target && target.dataset.url) {
    window.open(target.dataset.url, "_blank");
  }
});
