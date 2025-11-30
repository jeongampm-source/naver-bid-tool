# PART 1
import sys
import csv
import time
import hmac
import hashlib
import base64
import json
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from PyQt5.QtWidgets import (
    QApplication, QWidget, QPushButton, QLabel, QLineEdit, QTextEdit,
    QVBoxLayout, QHBoxLayout, QFileDialog, QGroupBox, QMessageBox,
    QGridLayout, QProgressBar, QComboBox, QTabWidget, QListWidget,
    QListWidgetItem
)
from PyQt5.QtCore import QThread, pyqtSignal, QTimer, Qt

class NaverAdApi:
    def __init__(self, base_url, api_key, secret_key, customer_id):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.secret_key = secret_key
        self.customer_id = customer_id

    def _sig(self, ts, method, uri):
        msg = f"{ts}.{method}.{uri}"
        h = hmac.new(self.secret_key.encode(), msg.encode(), hashlib.sha256)
        return base64.b64encode(h.digest()).decode()

    def _headers(self, method, uri):
        ts = str(int(time.time() * 1000))
        return {
            "Content-Type": "application/json; charset=UTF-8",
            "X-Timestamp": ts,
            "X-API-KEY": self.api_key,
            "X-Customer": self.customer_id,
            "X-Signature": self._sig(ts, method, uri),
        }

    def update_bid(self, adg, pc, mo):
        uri = f"/ncc/adgroups/{adg}"
        url = self.base_url + uri
        r = requests.get(url, headers=self._headers("GET", uri))
        if r.status_code != 200:
            raise Exception(f"GET 실패 {adg}: {r.text}")
        data = r.json()
        if pc is not None:
            data["pcNetworkBidWeight"] = int(pc)
        if mo is not None:
            data["mobileNetworkBidWeight"] = int(mo)
        r = requests.put(url, headers=self._headers("PUT", uri), data=json.dumps(data))
        if r.status_code != 200:
            raise Exception(f"PUT 실패 {adg}: {r.text}")
        return r.json()

def read_csv(path):
    last = None
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            with open(path, "r", encoding=enc) as f:
                return list(csv.DictReader(f))
        except Exception as e:
            last = e
    raise Exception(f"CSV 인코딩 오류: {last}")

class BidWorker(QThread):
    progress = pyqtSignal(int, int)
    log = pyqtSignal(str)
    stats = pyqtSignal(int, int, float, float)
    finished_signal = pyqtSignal()

    def __init__(self, api, tasks, threads):
        super().__init__()
        self.api = api
        self.tasks = tasks
        self.threads = threads
        self._stop = False

    def stop(self):
        self._stop = True

    def run(self):
        total = len(self.tasks)
        done = 0
        start = time.time()
        tasks = self.tasks
        while tasks and not self._stop:
            retry = []
            with ThreadPoolExecutor(max_workers=self.threads) as ex:
                futs = {ex.submit(self.proc, a, p, m): (a, p, m) for (a, p, m) in tasks}
                for f in as_completed(futs):
                    if self._stop:
                        break
                    adg, pc, mo = futs[f]
                    try:
                        f.result()
                        done += 1
                        self.log.emit(f"✔ {adg} 성공")
                    except Exception as e:
                        retry.append((adg, pc, mo))
                        self.log.emit(f"✖ {adg} 실패: {e}")
                    elapsed = max(time.time() - start, 0.0001)
                    qps = done / elapsed
                    eta = (total - done) / qps if qps > 0 else -1
                    self.progress.emit(done, total)
                    self.stats.emit(done, total, qps, eta)
            tasks = retry
            if not tasks:
                break
            time.sleep(1)
        self.finished_signal.emit()

    def proc(self, adg, pc, mo):
        return self.api.update_bid(adg, pc, mo)

# PART 2

class NaverBidSchedulerGUI(QWidget):
    def __init__(self):
        super().__init__()

        self.worker = None
        self.csv_path_tab1 = None
        self.report_csv_path = None

        self.schedules = []
        self.schedule_enabled = False
        self.schedule_history = set()

        self.timer = QTimer()
        self.timer.setInterval(30000)
        self.timer.timeout.connect(self.schedule_tick)

        self.accounts = {}
        self.accounts_file = "accounts.json"
        self.schedules_file = "schedules.json"

        self.init_ui()
        self.load_accounts()
        self.load_schedules()
        self.timer.start()

    def init_ui(self):
        self.setWindowTitle("네이버 검색광고 – 자동 입찰가중치 관리")
        self.resize(1100, 900)

        main = QVBoxLayout()

        acc_g = QGroupBox("계정 프로파일")
        acc = QGridLayout()
        self.account_combo = QComboBox()
        self.account_name_edit = QLineEdit()
        btn_save = QPushButton("계정 저장/업데이트")
        btn_del = QPushButton("계정 삭제")

        btn_save.clicked.connect(self.save_account)
        btn_del.clicked.connect(self.delete_account)
        self.account_combo.currentIndexChanged.connect(self.select_account)

        acc.addWidget(QLabel("계정 선택"), 0, 0)
        acc.addWidget(self.account_combo, 0, 1)
        acc.addWidget(QLabel("계정 이름"), 1, 0)
        acc.addWidget(self.account_name_edit, 1, 1)
        acc.addWidget(btn_save, 2, 0)
        acc.addWidget(btn_del, 2, 1)
        acc_g.setLayout(acc)
        main.addWidget(acc_g)

        api_g = QGroupBox("API 설정")
        api = QGridLayout()
        self.base_url_edit = QLineEdit("https://api.naver.com")
        self.api_key_edit = QLineEdit()
        self.secret_key_edit = QLineEdit()
        self.customer_id_edit = QLineEdit()

        api.addWidget(QLabel("BASE URL"), 0, 0)
        api.addWidget(self.base_url_edit, 0, 1)
        api.addWidget(QLabel("API KEY"), 1, 0)
        api.addWidget(self.api_key_edit, 1, 1)
        api.addWidget(QLabel("SECRET KEY"), 2, 0)
        api.addWidget(self.secret_key_edit, 2, 1)
        api.addWidget(QLabel("CUSTOMER ID"), 3, 0)
        api.addWidget(self.customer_id_edit, 3, 1)
        api_g.setLayout(api)
        main.addWidget(api_g)

        self.tabs = QTabWidget()
        self.tabs.addTab(self.build_tab1(), "CSV 실행")
        self.tabs.addTab(self.build_tab2(), "리포트 실행")
        self.tabs.addTab(self.build_tab3(), "스케줄링")
        main.addWidget(self.tabs)

        self.progress_bar = QProgressBar()
        main.addWidget(self.progress_bar)

        self.stats_label = QLabel("진행률 / 속도 / ETA")
        main.addWidget(self.stats_label)

        btn_stop = QPushButton("작업 중지")
        btn_stop.clicked.connect(self.stop_worker)
        main.addWidget(btn_stop)

        self.log_edit = QTextEdit()
        self.log_edit.setReadOnly(True)
        main.addWidget(self.log_edit)

        self.setLayout(main)

    def build_tab1(self):
        w = QWidget()
        v = QVBoxLayout()

        g = QGroupBox("CSV 선택")
        h = QHBoxLayout()
        self.tab1_csv_label = QLabel("파일 미선택")
        btn = QPushButton("선택")
        btn.clicked.connect(self.select_csv_tab1)
        h.addWidget(self.tab1_csv_label)
        h.addWidget(btn)
        g.setLayout(h)
        v.addWidget(g)

        g2 = QGroupBox("스레드")
        h2 = QHBoxLayout()
        h2.addWidget(QLabel("스레드 수"))
        self.thread_tab1 = QComboBox()
        for i in range(1, 17):
            self.thread_tab1.addItem(str(i))
        self.thread_tab1.setCurrentText("6")
        h2.addWidget(self.thread_tab1)
        g2.setLayout(h2)
        v.addWidget(g2)

        btn_run = QPushButton("CSV 실행")
        btn_run.clicked.connect(self.run_tab1)
        v.addWidget(btn_run)

        w.setLayout(v)
        return w

    def build_tab2(self):
        w = QWidget()
        v = QVBoxLayout()

        g = QGroupBox("리포트 CSV 선택")
        h = QHBoxLayout()
        self.tab2_csv_label = QLabel("파일 미선택")
        btn = QPushButton("선택")
        btn.clicked.connect(self.select_csv_tab2)
        h.addWidget(self.tab2_csv_label)
        h.addWidget(btn)
        g.setLayout(h)
        v.addWidget(g)

        btn_run = QPushButton("리포트 실행")
        btn_run.clicked.connect(self.run_tab2)
        v.addWidget(btn_run)

        w.setLayout(v)
        return w

    def build_tab3(self):
        w = QWidget()
        v = QVBoxLayout()

        g = QGroupBox("스케줄 추가")
        grid = QGridLayout()

        grid.addWidget(QLabel("CSV 파일"), 0, 0)
        self.tab3_csv_label = QLabel("파일 미선택")
        btn_csv = QPushButton("선택")
        btn_csv.clicked.connect(self.select_csv_tab3)
        grid.addWidget(self.tab3_csv_label, 0, 1)
        grid.addWidget(btn_csv, 0, 2)

        grid.addWidget(QLabel("계정 선택"), 1, 0)
        self.tab3_account_combo = QComboBox()
        grid.addWidget(self.tab3_account_combo, 1, 1, 1, 2)

        grid.addWidget(QLabel("요일"), 2, 0)
        self.tab3_day = QComboBox()
        self.tab3_day.addItems(["평일", "주말", "월", "화", "수", "목", "금", "토", "일"])
        grid.addWidget(self.tab3_day, 2, 1)

        grid.addWidget(QLabel("시간(HH:MM)"), 3, 0)
        self.tab3_time = QLineEdit()
        self.tab3_time.setPlaceholderText("예: 09:00")
        grid.addWidget(self.tab3_time, 3, 1)

        btn_add = QPushButton("스케줄 추가")
        btn_add.clicked.connect(self.add_schedule)
        grid.addWidget(btn_add, 4, 0, 1, 3)

        g.setLayout(grid)
        v.addWidget(g)

        g2 = QGroupBox("등록된 스케줄")
        v2 = QVBoxLayout()
        self.tab3_list = QListWidget()
        self.tab3_list.itemChanged.connect(self.toggle_schedule_state)
        v2.addWidget(self.tab3_list)

        btn_del = QPushButton("선택 스케줄 삭제")
        btn_del.clicked.connect(self.delete_schedule)
        v2.addWidget(btn_del)

        g2.setLayout(v2)
        v.addWidget(g2)

        h = QHBoxLayout()
        btn_start = QPushButton("스케줄링 시작")
        btn_stop = QPushButton("스케줄링 중지")
        btn_start.clicked.connect(self.start_scheduler)
        btn_stop.clicked.connect(self.stop_scheduler)
        h.addWidget(btn_start)
        h.addWidget(btn_stop)

        v.addLayout(h)

        w.setLayout(v)
        return w

# PART 3

    def save_account(self):
        name = self.account_name_edit.text().strip()
        if not name:
            return
        self.accounts[name] = {
            "base_url": self.base_url_edit.text(),
            "api_key": self.api_key_edit.text(),
            "secret_key": self.secret_key_edit.text(),
            "customer_id": self.customer_id_edit.text(),
        }
        with open(self.accounts_file, "w", encoding="utf-8") as f:
            json.dump(self.accounts, f, ensure_ascii=False, indent=2)
        self.load_accounts()
        self.account_combo.setCurrentText(name)
        self.log(f"계정 '{name}' 저장됨")
        self.refresh_schedule_account_combo()

    def delete_account(self):
        name = self.account_combo.currentText()
        if name in self.accounts:
            del self.accounts[name]
            with open(self.accounts_file, "w", encoding="utf-8") as f:
                json.dump(self.accounts, f, ensure_ascii=False, indent=2)
            self.load_accounts()
            self.log(f"계정 '{name}' 삭제됨")

    def load_accounts(self):
        try:
            with open(self.accounts_file, "r", encoding="utf-8") as f:
                self.accounts = json.load(f)
        except:
            self.accounts = {}
        self.account_combo.clear()
        self.account_combo.addItem("새 계정 (직접 입력)")
        for name in self.accounts.keys():
            self.account_combo.addItem(name)
        self.refresh_schedule_account_combo()

    def refresh_schedule_account_combo(self):
        current = self.tab3_account_combo.currentText() if hasattr(self, "tab3_account_combo") else None
        self.tab3_account_combo.clear()
        self.tab3_account_combo.addItem("현재 입력값")
        for name in self.accounts.keys():
            self.tab3_account_combo.addItem(name)
        if current:
            self.tab3_account_combo.setCurrentText(current)

    def select_account(self, idx):
        if idx <= 0:
            self.account_name_edit.clear()
            self.base_url_edit.setText("https://api.naver.com")
            self.api_key_edit.clear()
            self.secret_key_edit.clear()
            self.customer_id_edit.clear()
            return
        name = self.account_combo.currentText()
        data = self.accounts.get(name, {})
        self.account_name_edit.setText(name)
        self.base_url_edit.setText(data.get("base_url", "https://api.naver.com"))
        self.api_key_edit.setText(data.get("api_key", ""))
        self.secret_key_edit.setText(data.get("secret_key", ""))
        self.customer_id_edit.setText(str(data.get("customer_id", "")))

    def log(self, msg):
        now = datetime.now().strftime("%H:%M:%S")
        self.log_edit.append(f"[{now}] {msg}")

    def update_progress(self, done, total):
        pct = int(done / total * 100) if total else 0
        self.progress_bar.setValue(pct)

    def update_stats(self, d, t, q, eta):
        if eta < 0:
            txt = "ETA 계산중"
        else:
            m = int(eta // 60)
            s = int(eta % 60)
            txt = f"ETA {m}m {s}s"
        self.stats_label.setText(f"완료 {d}/{t} | 속도 {q:.2f}/s | {txt}")

    def worker_finished(self):
        self.log("✔ 작업 종료")

    def stop_worker(self):
        if self.worker and self.worker.isRunning():
            self.worker.stop()
            self.log("작업 중지 요청됨")

    def select_csv_tab1(self):
        path, _ = QFileDialog.getOpenFileName(self, "CSV 선택", "", "CSV (*.csv)")
        if path:
            self.csv_path_tab1 = path
            self.tab1_csv_label.setText(path)

    def select_csv_tab2(self):
        path, _ = QFileDialog.getOpenFileName(self, "리포트 CSV 선택", "", "CSV (*.csv)")
        if path:
            self.report_csv_path = path
            self.tab2_csv_label.setText(path)

    def select_csv_tab3(self):
        path, _ = QFileDialog.getOpenFileName(self, "스케줄 CSV 선택", "", "CSV (*.csv)")
        if path:
            self.tab3_selected_csv = path
            self.tab3_csv_label.setText(path)

    def run_tab1(self):
        if not self.csv_path_tab1:
            QMessageBox.warning(self, "오류", "CSV 파일을 선택하세요.")
            return
        rows = read_csv(self.csv_path_tab1)
        tasks = []
        for r in rows:
            adg = (r.get("adgroup_id") or "").strip()
            pc = r.get("pc")
            mo = r.get("mo")
            pc = int(pc) if pc else None
            mo = int(mo) if mo else None
            if adg and (pc or mo):
                tasks.append((adg, pc, mo))
        api = self.build_api()
        threads = int(self.thread_tab1.currentText())
        self.start_worker(api, tasks, threads)

    def run_tab2(self):
        QMessageBox.information(self, "안내", "Tab2 기능은 유지(보류) 상태입니다.")

    def build_api(self):
        base = self.base_url_edit.text().strip()
        key = self.api_key_edit.text().strip()
        sec = self.secret_key_edit.text().strip()
        cid = self.customer_id_edit.text().strip()
        if not (base and key and sec and cid):
            raise Exception("API 정보 입력 필요")
        return NaverAdApi(base, key, sec, cid)

    def start_worker(self, api, tasks, threads):
        self.worker = BidWorker(api, tasks, threads)
        self.worker.log.connect(self.log)
        self.worker.progress.connect(self.update_progress)
        self.worker.stats.connect(self.update_stats)
        self.worker.finished_signal.connect(self.worker_finished)
        self.worker.start()

    def add_schedule(self):
        if not hasattr(self, "tab3_selected_csv"):
            QMessageBox.warning(self, "오류", "CSV 파일을 선택하세요.")
            return

        time_txt = self.tab3_time.text().strip()
        if not (len(time_txt) == 5 and time_txt[2] == ":"):
            QMessageBox.warning(self, "오류", "시간 형식 HH:MM 필요")
            return

        day = self.tab3_day.currentText()
        csv_path = self.tab3_selected_csv

        acc_name = self.tab3_account_combo.currentText()
        if acc_name and acc_name != "현재 입력값":
            acc_data = self.accounts.get(acc_name)
            if not acc_data:
                QMessageBox.warning(self, "오류", "선택한 계정 정보를 찾을 수 없습니다.")
                return
        else:
            acc_data = {
                "base_url": self.base_url_edit.text(),
                "api_key": self.api_key_edit.text(),
                "secret_key": self.secret_key_edit.text(),
                "customer_id": self.customer_id_edit.text(),
            }
            acc_name = "현재 입력값"

        for k, v in acc_data.items():
            if not v:
                QMessageBox.warning(self, "오류", "계정 정보가 모두 입력되어야 합니다.")
                return

        sch = {
            "csv": csv_path,
            "day": day,
            "time": time_txt,
            "enabled": True,
            "account": {
                "name": acc_name,
                "base_url": acc_data.get("base_url", ""),
                "api_key": acc_data.get("api_key", ""),
                "secret_key": acc_data.get("secret_key", ""),
                "customer_id": acc_data.get("customer_id", ""),
            },
        }

        item = QListWidgetItem(self.format_schedule_label(sch))
        item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
        item.setCheckState(Qt.Checked)
        item.setData(Qt.UserRole, sch)

        self.tab3_list.addItem(item)
        self.schedules.append(sch)
        self.log(f"스케줄 추가됨 → {day} {time_txt}")
        self.save_schedules()

    def delete_schedule(self):
        row = self.tab3_list.currentRow()
        if row < 0:
            return
        self.tab3_list.takeItem(row)
        del self.schedules[row]
        self.log("스케줄 삭제됨")
        self.save_schedules()

    def start_scheduler(self):
        self.schedule_enabled = True
        self.log("스케줄링 시작됨")

    def stop_scheduler(self):
        self.schedule_enabled = False
        self.log("스케줄링 중지됨")

    def schedule_tick(self):
        if not self.schedule_enabled:
            return
        now = datetime.now()
        tm = now.strftime("%H:%M")
        wd = now.weekday()

        for i in range(self.tab3_list.count()):
            item = self.tab3_list.item(i)
            if item.checkState() != Qt.Checked:
                continue
            sch = item.data(Qt.UserRole)

            if not self.match_day(sch["day"], wd):
                continue
            if sch["time"] != tm:
                continue

            key = f"{sch['csv']}|{sch['day']}|{sch['time']}|{now.strftime('%Y-%m-%d')}"
            if key in self.schedule_history:
                continue
            self.schedule_history.add(key)

            self.run_schedule(sch)

    def match_day(self, d, wd):
        if d == "평일":
            return wd <= 4
        if d == "주말":
            return wd >= 5
        mp = {"월":0,"화":1,"수":2,"목":3,"금":4,"토":5,"일":6}
        return mp.get(d) == wd

    def run_schedule(self, sch):
        self.log(f"⏰ 스케줄 실행 → {sch['day']} {sch['time']}")
        rows = read_csv(sch["csv"])
        tasks = []
        for r in rows:
            adg = (r.get("adgroup_id") or "").strip()
            pc = r.get("pc")
            mo = r.get("mo")
            pc = int(pc) if pc else None
            mo = int(mo) if mo else None
            if adg and (pc or mo):
                tasks.append((adg, pc, mo))
        acc = sch.get("account", {})
        required = [acc.get("base_url"), acc.get("api_key"), acc.get("secret_key"), acc.get("customer_id")]
        if not all(required):
            self.log("✖ 스케줄 계정 정보가 완전하지 않아 실행을 건너뜁니다.")
            return
        api = NaverAdApi(
            acc.get("base_url", "https://api.naver.com"),
            acc.get("api_key", ""),
            acc.get("secret_key", ""),
            str(acc.get("customer_id", "")),
        )
        threads = int(self.thread_tab1.currentText())
        self.start_worker(api, tasks, threads)

    def format_schedule_label(self, sch):
        acc_name = sch.get("account", {}).get("name", "")
        return f"[{sch['day']} {sch['time']}] ({acc_name}) {sch['csv']}"

    def load_schedules(self):
        updated = False
        try:
            with open(self.schedules_file, "r", encoding="utf-8") as f:
                self.schedules = json.load(f)
        except Exception:
            self.schedules = []

        def ensure_account_fields(sch):
            acc = sch.get("account") or {}
            if not acc:
                acc = {
                    "name": "현재 입력값",
                    "base_url": self.base_url_edit.text(),
                    "api_key": self.api_key_edit.text(),
                    "secret_key": self.secret_key_edit.text(),
                    "customer_id": self.customer_id_edit.text(),
                }
                sch["account"] = acc
                return True

            changed = False
            for key in ("base_url", "api_key", "secret_key", "customer_id"):
                if key not in acc:
                    acc[key] = ""
                    changed = True
            if "name" not in acc:
                acc["name"] = ""
                changed = True
            return changed

        self.tab3_list.blockSignals(True)
        self.tab3_list.clear()
        for sch in self.schedules:
            if ensure_account_fields(sch):
                updated = True
            item = QListWidgetItem(self.format_schedule_label(sch))
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Checked if sch.get("enabled", True) else Qt.Unchecked)
            item.setData(Qt.UserRole, sch)
            self.tab3_list.addItem(item)
        self.tab3_list.blockSignals(False)

        if updated:
            self.save_schedules()

    def save_schedules(self):
        with open(self.schedules_file, "w", encoding="utf-8") as f:
            json.dump(self.schedules, f, ensure_ascii=False, indent=2)

    def toggle_schedule_state(self, item):
        idx = self.tab3_list.row(item)
        if 0 <= idx < len(self.schedules):
            self.schedules[idx]["enabled"] = item.checkState() == Qt.Checked
            self.save_schedules()

def main():
    app = QApplication(sys.argv)
    gui = NaverBidSchedulerGUI()
    gui.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
