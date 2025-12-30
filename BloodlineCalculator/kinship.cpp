//====================================================================
//  BloodlineCalculator  ―― 血量 + 近交(kinship/F) を高速出力
//
//  Blood File-A : 行 = 全馬,   列 = 対象 (対象馬の血が何 % 含まれるか)   = getBlood(row, target)
//  Blood File-B : 行 = 対象,   列 = 全馬 (各馬の血が何 % 含まれるか)     = getBlood(target, col)
//
//  Kin   File-A : 行 = 全馬,   列 = 対象 (row×target を親にした子の近交係数F) = kinship(row, target)
//  Kin   File-B : 行 = 対象,   列 = 全馬 (同上)                             = kinship(target, col)
//
//  ★ 血量(getBlood)は「このまま」：ロジックは一切改変しない
//  ★ ただし “表示” として 自分対自分は 0 を出力（血量/近交とも）
//
//  RocksDB キャッシュ：
//    血量  : "<tgt>|<anc>"
//    Kin   : "K2|<a>|<b>"  (a<=b 正規化)   // K| だと過去キャッシュ汚染の可能性があるので K2|
//    F(i)  : "F2|<i>"                    // 同上
//====================================================================

#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <vector>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <tuple>
#include <algorithm>
#include <climits>
#include <cmath>
#include <regex>
#include <functional>
#include <deque>
#include <cctype>
#include <memory>

#define NOMINMAX
#ifdef _WIN32
#include <windows.h>
#include <psapi.h>
#else
#include <sys/resource.h>
#include <unistd.h>
#endif

#include <rocksdb/db.h>
#include <rocksdb/options.h>

// --------------- util -----------------
static inline std::string trim(std::string s) {
    size_t b = s.find_first_not_of(" \t\r\n");
    size_t e = s.find_last_not_of(" \t\r\n");
    return (b == std::string::npos) ? "" : s.substr(b, e - b + 1);
}

std::unique_ptr<rocksdb::DB> db;
constexpr const char* DBPATH = "D:/AI/C++/blood_cache_db";

//------------------------- 定数 -------------------------------
static const std::string UNKNOWN_SIRE = "UNKNOWN_SIRE";
static const std::string UNKNOWN_DAM = "UNKNOWN_DAM";

//------------------------- 構造体 -------------------------------
struct Horse {
    std::string PrimaryKey;
    std::string HorseName;
    std::string YearStr;
    int         YearInt;
    std::string Sire;
    std::string Dam;
};

//------------------------- 大域 -------------------------------
std::unordered_map<std::string, Horse> horses;        // PK → 馬
std::unordered_map<std::string, std::string> keyToDisplayName;
std::unordered_map<std::string, std::vector<std::string>> childrenOf; // 親→子

//------------------------- ユーティリティ -------------------------------
size_t getMemoryUsageMB() {
#ifdef _WIN32
    PROCESS_MEMORY_COUNTERS_EX pmc;
    if (GetProcessMemoryInfo(GetCurrentProcess(),
        (PROCESS_MEMORY_COUNTERS*)&pmc,
        sizeof(pmc)))
        return pmc.WorkingSetSize / (1024 * 1024);
#else
    struct rusage u;
    if (!getrusage(RUSAGE_SELF, &u))
        return u.ru_maxrss / 1024;
#endif
    return 0;
}

int parseYearInt(const std::string& y) {
    if (y.empty()) return INT_MIN;
    try { return std::stoi(y); }
    catch (...) { return INT_MIN; }
}

std::vector<std::string> splitCSV(const std::string& line) {
    std::vector<std::string> out;  bool inq = false;  std::string buf;
    for (char c : line) {
        if (c == '"') { inq = !inq; }
        else if (c == ',' && !inq) { out.push_back(buf); buf.clear(); }
        else buf.push_back(c);
    }
    out.push_back(buf);
    return out;
}

void openDB() {
    rocksdb::Options op;
    op.create_if_missing = true;
    op.compression = rocksdb::kNoCompression;
    op.IncreaseParallelism();
    op.OptimizeLevelStyleCompaction();

    rocksdb::DB* raw = nullptr;
    auto st = rocksdb::DB::Open(op, DBPATH, &raw);
    if (!st.ok()) { std::cerr << st.ToString() << '\n'; exit(1); }
    db.reset(raw);
}

//========================= LRU (blood) =========================
constexpr size_t LRU_LIMIT = 200'000;

struct LRUKey { std::string tgt, anc; };
struct KeyHash {
    size_t operator()(const LRUKey& k) const {
        return std::hash<std::string>()(k.tgt) ^ (std::hash<std::string>()(k.anc) << 1);
    }
};
struct KeyEq {
    bool operator()(const LRUKey& a, const LRUKey& b) const {
        return a.tgt == b.tgt && a.anc == b.anc;
    }
};

std::unordered_map<LRUKey, double, KeyHash, KeyEq> lru;
std::deque<LRUKey> order;

void lruPut(const LRUKey& k, double v) {
    lru[k] = v;
    order.push_back(k);
    if (order.size() > LRU_LIMIT) {
        lru.erase(order.front());
        order.pop_front();
    }
}
bool lruGet(const LRUKey& k, double& v) {
    auto it = lru.find(k);
    if (it == lru.end()) return false;
    v = it->second;
    return true;
}

//------------------------- CSV 読込 -------------------------------
void loadBloodlineCSV(const std::string& f) {
    std::ifstream ifs(f);
    if (!ifs) { std::cerr << "cannot open " << f << '\n'; exit(1); }
    std::string head; getline(ifs, head);

    std::string ln; int cnt = 0;
    while (getline(ifs, ln)) {
        if (ln.empty()) continue;
        auto c = splitCSV(ln);
        if (c.size() < 9) continue;

        Horse h;
        h.PrimaryKey = c[0];
        h.Sire = c[1].empty() ? UNKNOWN_SIRE : c[1];
        h.Dam = c[2].empty() ? UNKNOWN_DAM : c[2];
        h.YearStr = c[5];
        h.YearInt = parseYearInt(c[5]);
        h.HorseName = c[8];

        horses[h.PrimaryKey] = h;
        keyToDisplayName[h.PrimaryKey] = h.HorseName + " [" + h.YearStr + "]";

        childrenOf[h.Sire].push_back(h.PrimaryKey);
        childrenOf[h.Dam].push_back(h.PrimaryKey);
        ++cnt;
    }
    std::cout << "[load] " << cnt << " rows, horses=" << horses.size() << '\n';
}

//------------------------- 血量（RocksDB + LRU） -------------------------------
// ★ここはユーザー指定により “そのまま” 残す
double getBlood(const std::string& tgt, const std::string& anc,
    std::unordered_set<std::string>& stk)
{
    LRUKey key{ tgt, anc };
    double val;

    // 1) LRU
    if (lruGet(key, val)) return val;

    // 2) RocksDB
    std::string vstr;
    if (db->Get(rocksdb::ReadOptions(), tgt + "|" + anc, &vstr).ok()) {
        val = std::stod(vstr);
        lruPut(key, val);
        return val;
    }

    // 3) 再帰
    if (tgt == UNKNOWN_SIRE || tgt == UNKNOWN_DAM || !horses.count(tgt))
        val = 0.0;
    else if (tgt == anc) val = 1.0;
    else if (stk.count(tgt)) val = 0.0;
    else {
        stk.insert(tgt);
        const Horse& h = horses[tgt];
        val = 0.5 * getBlood(h.Sire, anc, stk) + 0.5 * getBlood(h.Dam, anc, stk);
        stk.erase(tgt);
    }

    db->Put(rocksdb::WriteOptions(), tgt + "|" + anc, std::to_string(val));
    lruPut(key, val);
    return val;
}

//------------------------- 祖先・子孫 -------------------------------
void collectAncestors(const std::string& pk, std::unordered_set<std::string>& s) {
    if (pk == UNKNOWN_SIRE || pk == UNKNOWN_DAM || !horses.count(pk)) return;

    const Horse& h = horses[pk];
    for (const auto& p : { h.Sire, h.Dam }) {
        if (s.insert(p).second) collectAncestors(p, s);
    }
}

void collectDescendants(const std::string& pk, std::unordered_set<std::string>& s) {
    std::queue<std::string> q;
    q.push(pk);
    while (!q.empty()) {
        std::string cur = q.front(); q.pop();
        for (const auto& ch : childrenOf[cur]) {
            if (s.insert(ch).second) q.push(ch);
        }
    }
}

//--------------------------------------------------------------------
// 行列 CSV 出力（血量/近交 共通で使える）
//  - calcFilter が true のときだけ compute(row,col) を計算
//  - ★自分対自分は出力上 0（表示だけ）
//--------------------------------------------------------------------
template <class ComputeFn>
void saveCSVMatrix_Generic(const std::string& filename,
    const std::vector<std::string>& rowKeys,
    const std::vector<std::string>& colKeys,
    const std::function<bool(const std::string&, const std::string&)>& calcFilter,
    ComputeFn compute,
    const char* logPrefix)
{
    if (rowKeys.empty() || colKeys.empty()) {
        std::cerr << "[" << logPrefix << "] rows/cols empty → skip\n";
        return;
    }

    std::ofstream ofs(filename);
    if (!ofs) { std::cerr << "cannot open " << filename << '\n'; return; }
    ofs << std::fixed << std::setprecision(8);

    ofs << "HorseName";
    for (const auto& ck : colKeys) ofs << ',' << keyToDisplayName[ck];
    ofs << '\n';

    const size_t total = rowKeys.size();
    size_t idx = 0;

    for (const auto& rk : rowKeys) {
        ++idx;
        std::cout << "[" << logPrefix << "] (" << idx << '/' << total << ")  "
            << keyToDisplayName[rk] << '\n';

        ofs << keyToDisplayName[rk];

        for (const auto& ck : colKeys) {
            // ★自分対自分は常に 0（表示だけ）
            if (rk == ck) {
                ofs << ",0.00000000";
                continue;
            }

            double v = 0.0;
            if (calcFilter(rk, ck)) {
                v = compute(rk, ck);
                if (std::fabs(v) < 1e-12) v = 0.0;
            }
            ofs << ',' << v;
        }
        ofs << '\n';
    }

    std::cout << "[" << logPrefix << "] " << filename << " 出力完了\n";
}

//-------------------------------------------------------------
// 血量（targets=1 のとき）
//  ★自分行は出力 0（表示だけ）
//-------------------------------------------------------------
void saveDescFast(const std::string& out,
    const std::vector<std::string>& rows,
    const std::unordered_set<std::string>& setDesc,
    const std::string& target)
{
    std::ofstream ofs(out);
    ofs << std::fixed << std::setprecision(8);
    ofs << "HorseName," << keyToDisplayName[target] << '\n';

    const size_t total = rows.size();
    size_t idx = 0;
    for (const auto& rk : rows) {
        ++idx;

        // ★表示だけ 0
        if (rk == target) {
            std::cout << "[BloodA] (" << idx << '/' << total << ")  "
                << keyToDisplayName[rk] << "  [self=>0]\n";
            ofs << keyToDisplayName[rk] << ",0.00000000\n";
            continue;
        }

        double v = 0.0;
        bool needCalc = setDesc.count(rk);
        if (needCalc) {
            std::unordered_set<std::string> stk;
            v = getBlood(rk, target, stk);
            if (std::fabs(v) < 1e-12) v = 0.0;
        }

        double v_pct = std::floor(v * 1'000'000.0) / 10'000.0;
        std::cout << "[BloodA] (" << idx << '/' << total << ")  "
            << keyToDisplayName[rk] << "  "
            << (needCalc ? "[calc: " : "[skip: ")
            << std::fixed << std::setprecision(5) << v_pct << "%]\n";

        ofs << keyToDisplayName[rk] << ',' << std::setprecision(8) << v << '\n';
    }
}

void saveAncVert(const std::string& out,
    const std::vector<std::string>& all,
    const std::unordered_set<std::string>& setAnc,
    const std::string& target)
{
    std::ofstream ofs(out);
    ofs << std::fixed << std::setprecision(8);
    ofs << "HorseName," << keyToDisplayName[target] << '\n';

    const size_t total = all.size();
    size_t idx = 0;
    for (const auto& anc : all) {
        ++idx;

        // ★表示だけ 0
        if (anc == target) {
            std::cout << "[BloodB] (" << idx << '/' << total << ")  "
                << keyToDisplayName[anc] << "  [self=>0]\n";
            ofs << keyToDisplayName[anc] << ",0.00000000\n";
            continue;
        }

        double v = 0.0;
        bool needCalc = setAnc.count(anc);
        if (needCalc) {
            std::unordered_set<std::string> stk;
            v = getBlood(target, anc, stk);
            if (std::fabs(v) < 1e-12) v = 0.0;
        }

        double v_pct = std::floor(v * 1'000'000.0) / 10'000.0;
        std::cout << "[BloodB] (" << idx << '/' << total << ")  "
            << keyToDisplayName[anc] << "  "
            << (needCalc ? "[calc: " : "[skip: ")
            << std::fixed << std::setprecision(5) << v_pct << "%]\n";

        ofs << keyToDisplayName[anc] << ',' << std::setprecision(8) << v << '\n';
    }
}

// =====================================================================
// 近交（kinship / F）
// =====================================================================

// Founder判定（ここは kinship 用：血量には影響しない）
static inline bool isFounder(const std::string& pk) {
    if (pk == UNKNOWN_SIRE || pk == UNKNOWN_DAM) return true;
    auto it = horses.find(pk);
    if (it == horses.end()) return true;

    const Horse& h = it->second;
    if (h.Sire == UNKNOWN_SIRE || h.Dam == UNKNOWN_DAM) return true;
    if (!horses.count(h.Sire) || !horses.count(h.Dam)) return true;
    return false;
}

// ----- kin LRU -----
constexpr size_t LRU_KIN_LIMIT = 200'000;

struct PairKey { std::string a, b; }; // a<=b
struct PairHash {
    size_t operator()(const PairKey& k) const {
        return std::hash<std::string>()(k.a) ^ (std::hash<std::string>()(k.b) << 1);
    }
};
struct PairEq {
    bool operator()(const PairKey& x, const PairKey& y) const {
        return x.a == y.a && x.b == y.b;
    }
};

static inline PairKey makePairKey(std::string x, std::string y) {
    if (x > y) std::swap(x, y);
    return { x, y };
}

std::unordered_map<PairKey, double, PairHash, PairEq> kin_lru;
std::deque<PairKey> kin_order;

static inline void kinLruPut(const PairKey& k, double v) {
    kin_lru[k] = v;
    kin_order.push_back(k);
    if (kin_order.size() > LRU_KIN_LIMIT) {
        kin_lru.erase(kin_order.front());
        kin_order.pop_front();
    }
}
static inline bool kinLruGet(const PairKey& k, double& v) {
    auto it = kin_lru.find(k);
    if (it == kin_lru.end()) return false;
    v = it->second;
    return true;
}

// prefix: K2 / F2（過去キャッシュ汚染回避）
static inline std::string keyKin2(const std::string& a, const std::string& b) {
    auto k = makePairKey(a, b);
    return std::string("K2|") + k.a + "|" + k.b;
}
static inline std::string keyF2(const std::string& i) {
    return std::string("F2|") + i;
}

// forward
double getKinship(const std::string& x, const std::string& y,
    std::unordered_set<std::string>& guard);

double getInbreedingF(const std::string& i,
    std::unordered_set<std::string>& guardF);

// kinship φ(x,y)
//  - φ(i,i) = (1 + F(i)) / 2
//  - 子の近交係数: F(child of x×y) = φ(x,y)
double getKinship(const std::string& A, const std::string& B,
    std::unordered_set<std::string>& guard)
{
    PairKey pk = makePairKey(A, B);
    double val;

    // LRU
    if (kinLruGet(pk, val)) return val;

    // RocksDB
    std::string dbv;
    if (db->Get(rocksdb::ReadOptions(), keyKin2(pk.a, pk.b), &dbv).ok()) {
        val = std::stod(dbv);
        kinLruPut(pk, val);
        return val;
    }

    // Guard（無限ループ防止）
    std::string gkey = pk.a + "|" + pk.b;
    if (guard.count(gkey)) return 0.0;
    guard.insert(gkey);

    // founder / unknown
    if (isFounder(pk.a) && isFounder(pk.b)) {
        val = 0.0;
    }
    else if (pk.a == pk.b) {
        std::unordered_set<std::string> g2;
        double Fi = getInbreedingF(pk.a, g2);
        val = 0.5 * (1.0 + Fi);
    }
    else {
        // 片方でも未登録なら 0 とみなす
        auto itA = horses.find(pk.a);
        auto itB = horses.find(pk.b);
        if (itA == horses.end() || itB == horses.end()) {
            val = 0.0;
        }
        else {
            const Horse& hA = itA->second;
            const Horse& hB = itB->second;

            double k1 = getKinship(hA.Sire, hB.Sire, guard);
            double k2 = getKinship(hA.Sire, hB.Dam, guard);
            double k3 = getKinship(hA.Dam, hB.Sire, guard);
            double k4 = getKinship(hA.Dam, hB.Dam, guard);

            val = 0.25 * (k1 + k2 + k3 + k4);
        }
    }

    guard.erase(gkey);

    db->Put(rocksdb::WriteOptions(), keyKin2(pk.a, pk.b), std::to_string(val));
    kinLruPut(pk, val);
    return val;
}

// F(i) = φ(sire(i), dam(i))
double getInbreedingF(const std::string& i,
    std::unordered_set<std::string>& guardF)
{
    if (isFounder(i)) return 0.0;

    std::string dbv;
    if (db->Get(rocksdb::ReadOptions(), keyF2(i), &dbv).ok())
        return std::stod(dbv);

    if (guardF.count(i)) return 0.0;
    guardF.insert(i);

    auto it = horses.find(i);
    if (it == horses.end()) {
        guardF.erase(i);
        db->Put(rocksdb::WriteOptions(), keyF2(i), "0.0");
        return 0.0;
    }

    const Horse& h = it->second;
    std::unordered_set<std::string> g;
    double Fi = getKinship(h.Sire, h.Dam, g);

    guardF.erase(i);

    db->Put(rocksdb::WriteOptions(), keyF2(i), std::to_string(Fi));
    return Fi;
}

// targets と血縁になり得る集合（kinship の skip 用）
std::unordered_set<std::string> buildRelatedUniverse(const std::vector<std::string>& targetPks) {
    std::unordered_set<std::string> src;

    for (const auto& pk : targetPks) {
        if (pk == UNKNOWN_SIRE || pk == UNKNOWN_DAM) continue;
        if (!horses.count(pk)) continue;
        src.insert(pk);
        collectAncestors(pk, src);
    }

    std::unordered_set<std::string> rel = src;
    std::queue<std::string> q;
    for (const auto& s : src) q.push(s);

    while (!q.empty()) {
        std::string cur = q.front(); q.pop();
        auto it = childrenOf.find(cur);
        if (it == childrenOf.end()) continue;

        for (const auto& ch : it->second) {
            if (ch == UNKNOWN_SIRE || ch == UNKNOWN_DAM) continue;
            if (!horses.count(ch)) continue;
            if (rel.insert(ch).second) q.push(ch);
        }
    }
    return rel;
}

// =========================== main ===========================
int main() {
    loadBloodlineCSV("D:\\AI\\C++\\input\\bloodline.csv");

    std::cout << "対象馬 (年/年レンジ/PrimaryKey をカンマ区切り): ";
    std::string raw;  std::getline(std::cin, raw);

    openDB();

    // 1) targets を作る
    std::unordered_set<std::string> targetSet;
    std::vector<std::string> idTokens;

    std::regex reRange(R"(^(\d{4})-(\d{4})$)");
    std::regex reYear(R"(^(\d{4})$)");

    std::stringstream ss(raw);
    for (std::string tok; std::getline(ss, tok, ','); ) {
        tok = trim(tok);
        if (tok.empty()) continue;
        idTokens.push_back(tok);

        std::smatch m;
        if (std::regex_match(tok, m, reRange)) {
            int y1 = std::stoi(m[1]), y2 = std::stoi(m[2]);
            if (y1 > y2) std::swap(y1, y2);
            for (auto& kv : horses) {
                int y = kv.second.YearInt;
                if (y >= y1 && y <= y2) targetSet.insert(kv.first);
            }
        }
        else if (std::regex_match(tok, m, reYear)) {
            int y = std::stoi(tok);
            for (auto& kv : horses)
                if (kv.second.YearInt == y)
                    targetSet.insert(kv.first);
        }
        else {
            if (!horses.count(tok))
                std::cerr << "PrimaryKey \"" << tok << "\" not found - skip\n";
            else
                targetSet.insert(tok);
        }
    }

    std::cout << "[main] " << targetSet.size() << " targets found\n";
    if (targetSet.empty()) { std::cerr << "対象馬が 0 頭でした。\n"; return 1; }

    std::vector<std::string> targetPks(targetSet.begin(), targetSet.end());
    std::sort(targetPks.begin(), targetPks.end()); // 並び固定

    // 2) idLabel
    std::string idLabel;
    for (size_t i = 0; i < idTokens.size(); ++i) {
        if (i) idLabel += "_";
        idLabel += idTokens[i];
    }
    std::replace_if(idLabel.begin(), idLabel.end(),
        [](char c) { return !std::isalnum((unsigned char)c); }, '_');
    std::replace(idLabel.begin(), idLabel.end(), ' ', '_');

    // 3) allKeys（年代順）
    std::vector<std::string> allKeys;
    allKeys.reserve(horses.size());
    for (auto& kv : horses) allKeys.push_back(kv.first);

    std::sort(allKeys.begin(), allKeys.end(),
        [&](const std::string& a, const std::string& b) {
            int ya = horses[a].YearInt, yb = horses[b].YearInt;
            return (ya == yb) ? a < b : ya < yb;
        });

    // 4) blood 用 祖先/子孫セット（※血量はこのままの skip 条件を維持）
    std::unordered_set<std::string> setAnc, setDesc;
    for (const auto& pk : targetPks) {
        collectAncestors(pk, setAnc);
        collectDescendants(pk, setDesc);
    }

    // ==========================================================
    // Blood File-A / File-B
    // ==========================================================
    std::string fileA = "D:/AI/C++/out/blood_of_" + idLabel + "_in_all_horses.csv";
    std::string fileB = "D:/AI/C++/out/blood_of_all_horses_in_" + idLabel + ".csv";

    // File-A: 行=全馬, 列=targets, value=getBlood(row, target)
    if (targetPks.size() == 1) {
        saveDescFast(fileA, allKeys, setDesc, targetPks[0]);
    }
    else {
        saveCSVMatrix_Generic(fileA,
            allKeys, targetPks,
            [&](const std::string& row, const std::string&) {
                return setDesc.count(row) > 0;
            },
            [&](const std::string& row, const std::string& col) {
                std::unordered_set<std::string> stk;
                return getBlood(row, col, stk);
            },
            "BloodA");
    }
    std::cout << "[done] " << fileA << '\n';

    // File-B: 行=targets, 列=全馬, value=getBlood(target, col)
    if (targetPks.size() == 1) {
        saveAncVert(fileB, allKeys, setAnc, targetPks[0]);
    }
    else {
        saveCSVMatrix_Generic(fileB,
            targetPks, allKeys,
            [&](const std::string&, const std::string& col) {
                return setAnc.count(col) > 0;
            },
            [&](const std::string& row, const std::string& col) {
                std::unordered_set<std::string> stk;
                return getBlood(row, col, stk);
            },
            "BloodB");
    }
    std::cout << "[done] " << fileB << '\n';

    // ==========================================================
    // Kinship File-A / File-B
    // ==========================================================
    auto setRel = buildRelatedUniverse(targetPks);

    std::string fileKB = "D:/AI/C++/out/kinship_of_" + idLabel + "_with_all_horses.csv";
    std::string fileKA = "D:/AI/C++/out/kinship_of_all_horses_with_" + idLabel + ".csv";

    // Kin File-A: 行=全馬, 列=targets, value=φ(row, target)
    saveCSVMatrix_Generic(fileKA,
        allKeys, targetPks,
        [&](const std::string& row, const std::string& col) {
            return (setRel.count(row) > 0) && (setRel.count(col) > 0);
        },
        [&](const std::string& row, const std::string& col) {
            std::unordered_set<std::string> st;
            return getKinship(row, col, st);
        },
        "KinA");
    std::cout << "[done] " << fileKA << '\n';

    // Kin File-B: 行=targets, 列=全馬, value=φ(target, col)
    saveCSVMatrix_Generic(fileKB,
        targetPks, allKeys,
        [&](const std::string& row, const std::string& col) {
            return (setRel.count(row) > 0) && (setRel.count(col) > 0);
        },
        [&](const std::string& row, const std::string& col) {
            std::unordered_set<std::string> st;
            return getKinship(row, col, st);
        },
        "KinB");
    std::cout << "[done] " << fileKB << '\n';

    // --- 終了処理 ---
    db->Flush(rocksdb::FlushOptions());
    db.reset();

    std::cout << "[main] すべて完了しました。\n";
    return 0;
}
