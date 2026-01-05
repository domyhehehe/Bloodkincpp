//====================================================================
//  InbreedingCalculator  (RocksDB cache + % logs)
//    - Inbreeding coefficient: F(X) = f(sire(X), dam(X))
//    - Kinship / Coancestry:   f(A,B)
//    - Outputs:
//        1) inbreeding_of_<idLabel>.csv        (targets list)
//        2) rel_all_to_<idLabel>.csv           (rows=all, cols=targets)  = f(row,col)
//        3) rel_<idLabel>_to_all.csv           (rows=targets, cols=all)  = f(row,col)
//        4) inbreeding_terms_of_<idLabel>.csv  (per target, per path-pair term)
//        5) inbreeding_terms_sum_of_<idLabel>.csv (per target, per ancestor aggregated)
//====================================================================
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <climits>
#include <regex>
#include <cmath>
#include <cctype>
#include <deque>
#include <functional>

// --- RocksDB ---
#include <rocksdb/db.h>
#include <rocksdb/options.h>

//------------------------- constants -------------------------------
static const std::string UNKNOWN_SIRE = "UNKNOWN_SIRE";
static const std::string UNKNOWN_DAM = "UNKNOWN_DAM";

// decomposition settings
static const int    MAX_PATH_DEPTH = 50;
static const size_t MAX_PATHS_EACH = 200000;

// RocksDB settings
static constexpr const char* DBPATH = "D:/AI/C++/inbreeding_cache_db";
static constexpr size_t LRU_LIMIT = 300000; // メモリに載せる最近値（好みで）

//------------------------- Horse struct ----------------------------
struct Horse {
    std::string PrimaryKey;
    std::string HorseName;
    std::string YearStr;
    int         YearInt = INT_MIN;
    std::string Sire;
    std::string Dam;
};

//------------------------- Globals ---------------------------------
std::unordered_map<std::string, Horse> horses;        // PK -> Horse
std::unordered_map<std::string, std::string> keyToDisplayName;

//------------------------- Utils -----------------------------------
static inline std::string trim(std::string s) {
    size_t b = s.find_first_not_of(" \t\r\n");
    size_t e = s.find_last_not_of(" \t\r\n");
    return (b == std::string::npos) ? "" : s.substr(b, e - b + 1);
}

static inline bool isUnknownKey(const std::string& k) {
    return (k == UNKNOWN_SIRE || k == UNKNOWN_DAM || k.empty());
}

static inline std::string safeLabel(std::string s) {
    for (char& c : s) {
        unsigned char uc = static_cast<unsigned char>(c);
        if (!std::isalnum(uc)) c = '_';
    }
    while (s.find("__") != std::string::npos) {
        s.replace(s.find("__"), 2, "_");
    }
    return s;
}
static inline bool hasKnownParentsInDB(const std::string& pk) {
    if (isUnknownKey(pk) || !horses.count(pk)) return false;
    const Horse& h = horses.at(pk);
    if (isUnknownKey(h.Sire) || isUnknownKey(h.Dam)) return false;
    if (!horses.count(h.Sire) || !horses.count(h.Dam)) return false;
    return true;
}
static inline std::string csvEscape(const std::string& s) {
    bool need = false;
    for (char c : s) {
        if (c == ',' || c == '"' || c == '\n' || c == '\r') { need = true; break; }
    }
    if (!need) return s;
    std::string r;
    r.reserve(s.size() + 8);
    r.push_back('"');
    for (char c : s) {
        if (c == '"') r += "\"\"";
        else r.push_back(c);
    }
    r.push_back('"');
    return r;
}
//=========================================================
//  Depth (generation level) : founders=0, child=1+max(parents)
//=========================================================
std::unordered_map<std::string, int> depth_cache;
static thread_local std::unordered_set<std::string> depth_stack;

int getDepth(const std::string& pk) {
    if (isUnknownKey(pk) || !horses.count(pk)) return 0;

    auto it = depth_cache.find(pk);
    if (it != depth_cache.end()) return it->second;

    if (depth_stack.count(pk)) return 0; // cycle safety
    depth_stack.insert(pk);

    int d = 0;
    if (hasKnownParentsInDB(pk)) {
        const Horse& h = horses.at(pk);
        int ds = getDepth(h.Sire);
        int dd = getDepth(h.Dam);
        d = 1 + std::max(ds, dd);
    }
    else {
        d = 0;
    }

    depth_stack.erase(pk);
    depth_cache[pk] = d;
    return d;
}

//=========================================================
//  Compute max depth required for a specific child
//  (full generations on both sire and dam side)
//=========================================================
int maxDepthForChild(const std::string& childPk) {
    if (!horses.count(childPk)) return 0;
    const auto& c = horses.at(childPk);

    int ds = getDepth(c.Sire);
    int dd = getDepth(c.Dam);

    // +1 で「子の世代」を含めた深さ
    return std::max(ds, dd) + 1;
}

int parseYearInt(const std::string& y) {
    if (y.empty()) return INT_MIN;
    try { return std::stoi(y); }
    catch (...) { return INT_MIN; }
}

std::vector<std::string> splitCSV(const std::string& line) {
    std::vector<std::string> out;
    bool inq = false;
    std::string buf;

    for (char c : line) {
        if (c == '"') inq = !inq;
        else if (c == ',' && !inq) { out.push_back(buf); buf.clear(); }
        else buf.push_back(c);
    }
    out.push_back(buf);
    return out;
}

//--------------- % LOG helper -----------------
static inline double toPercentTrunc5(double v01) {
    // v01 in [0,1]. 0.125 -> 12.50000 (%)
    // vを小数6桁で切り捨て→%に換算（あなたのblood側と同じ系）
    double pct = std::floor(v01 * 1'000'000.0) / 10'000.0;
    return pct;
}

//--------------- double <-> string (avoid to_string precision loss) -------------
static inline std::string d2s(double v) {
    std::ostringstream oss;
    oss << std::setprecision(17) << v;
    return oss.str();
}
static inline bool s2d(const std::string& s, double& out) {
    try { out = std::stod(s); return true; }
    catch (...) { return false; }
}

//=========================================================
//  RocksDB + LRU
//=========================================================
std::unique_ptr<rocksdb::DB> db;

struct LRUEntry { std::string key; double val; };

std::unordered_map<std::string, double> lru;
std::deque<std::string> order;

static inline void lruPut(const std::string& k, double v) {
    lru[k] = v;
    order.push_back(k);
    if (order.size() > LRU_LIMIT) {
        lru.erase(order.front());
        order.pop_front();
    }
}
static inline bool lruGet(const std::string& k, double& v) {
    auto it = lru.find(k);
    if (it == lru.end()) return false;
    v = it->second;
    return true;
}

static inline void openDB() {
    rocksdb::Options op;
    op.create_if_missing = true;
    op.compression = rocksdb::kNoCompression;
    op.IncreaseParallelism();
    op.OptimizeLevelStyleCompaction();

    rocksdb::DB* raw = nullptr;
    auto st = rocksdb::DB::Open(op, DBPATH, &raw);
    if (!st.ok()) {
        std::cerr << "[rocksdb] " << st.ToString() << "\n";
        std::exit(1);
    }
    db.reset(raw);
    std::cout << "[rocksdb] open ok: " << DBPATH << "\n";
}

static inline bool dbGetDouble(const std::string& key, double& v) {
    std::string s;
    if (db && db->Get(rocksdb::ReadOptions(), key, &s).ok()) {
        return s2d(s, v);
    }
    return false;
}

static inline void dbPutDouble(const std::string& key, double v) {
    if (!db) return;
    rocksdb::WriteOptions wo;
    wo.disableWAL = true; // 高速優先（必要ならfalseに）
    db->Put(wo, key, d2s(v));
}

// Key builders
static inline std::string kF(const std::string& pk) { return "F|" + pk; }
static inline std::string kK(const std::string& a, const std::string& b) { return "K|" + a + "|" + b; }

//=========================================================
//  load CSV
//=========================================================
void loadBloodlineCSV(const std::string& path) {
    std::ifstream ifs(path);
    if (!ifs) {
        std::cerr << "cannot open CSV: " << path << "\n";
        std::exit(1);
    }

    std::string head;
    std::getline(ifs, head);

    std::string line;
    int cnt = 0;
    while (std::getline(ifs, line)) {
        if (trim(line).empty()) continue;

        auto c = splitCSV(line);
        if (c.size() < 9) continue;

        Horse h;
        h.PrimaryKey = trim(c[0]);
        if (h.PrimaryKey.empty()) continue;

        h.Sire = trim(c[1]);
        h.Dam = trim(c[2]);
        h.YearStr = trim(c[5]);
        h.YearInt = parseYearInt(h.YearStr);
        h.HorseName = trim(c[8]);

        if (h.Sire.empty()) h.Sire = UNKNOWN_SIRE;
        if (h.Dam.empty())  h.Dam = UNKNOWN_DAM;

        // ★事故データ対策：自分自身を父母にしてる場合は founder 扱いに落とす
        if (h.Sire == h.PrimaryKey) h.Sire = UNKNOWN_SIRE;
        if (h.Dam == h.PrimaryKey) h.Dam = UNKNOWN_DAM;

        horses[h.PrimaryKey] = h;
        keyToDisplayName[h.PrimaryKey] =
            (h.HorseName.empty() ? h.PrimaryKey : h.HorseName) + " [" + h.YearStr + "]";
        cnt++;
    }

    std::cout << "[load] " << cnt << " rows, horses=" << horses.size() << "\n";

    // sanity: missing parent links
    size_t missingParentLinks = 0;
    for (auto const& kv : horses) {
        const Horse& h = kv.second;
        if (!isUnknownKey(h.Sire) && !horses.count(h.Sire)) missingParentLinks++;
        if (!isUnknownKey(h.Dam) && !horses.count(h.Dam))  missingParentLinks++;
    }
    std::cout << "[debug] missing parent links = " << missingParentLinks
        << " (Sire/Dam must be PrimaryKey)\n";
}

//=========================================================
//  Inbreeding & Kinship (with RocksDB cache)
//=========================================================

// in-memory caches (still useful)
std::unordered_map<std::string, double> F_cache;

// symmetric kinship cache
struct PairKey {
    std::string a, b;
};
struct PairHash {
    size_t operator()(PairKey const& k) const noexcept {
        static std::hash<std::string> h;
        size_t x = h(k.a), y = h(k.b);
        x ^= (y + 0x9e3779b97f4a7c15ULL + (x << 6) + (x >> 2));
        return x;
    }
};
struct PairEq {
    bool operator()(PairKey const& x, PairKey const& y) const noexcept {
        return x.a == y.a && x.b == y.b;
    }
};
//std::unordered_map<PairKey, double, PairHash, PairEq> kin_cache;

// recursion guards
static thread_local std::unordered_set<std::string> F_stack;
static thread_local std::unordered_set<std::string> kin_stack;

double getKinship(const std::string& a, const std::string& b);
double getInbreeding(const std::string& pk);



//---------------------------------------------------------
//  F(X) = f(Sire, Dam)
//  founder (unknown parent) => F=0
//---------------------------------------------------------
double getInbreeding(const std::string& pk) {
    // 1) in-memory
    if (auto it = F_cache.find(pk); it != F_cache.end()) return it->second;

    // 2) LRU
    double v;
    {
        std::string key = kF(pk);
        if (lruGet(key, v)) { F_cache[pk] = v; return v; }
        if (dbGetDouble(key, v)) { lruPut(key, v); F_cache[pk] = v; return v; }
    }

    // cycle guard
    if (F_stack.count(pk)) return (F_cache[pk] = 0.0);
    F_stack.insert(pk);

    if (!horses.count(pk) || !hasKnownParentsInDB(pk)) {
        F_stack.erase(pk);
        double z = 0.0;
        F_cache[pk] = z;
        lruPut(kF(pk), z);
        dbPutDouble(kF(pk), z);
        return z;
    }

    const Horse& h = horses.at(pk);
    double f = getKinship(h.Sire, h.Dam);

    F_cache[pk] = f;
    lruPut(kF(pk), f);
    dbPutDouble(kF(pk), f);

    F_stack.erase(pk);
    return f;
}

//---------------------------------------------------------
//  Kinship f(A,B)
//    - unknown / missing => 0
//    - A==B => (1 + F(A))/2
//    - else expand the side that has known parents
//---------------------------------------------------------
double getKinship(const std::string& A, const std::string& B) {
    if (isUnknownKey(A) || isUnknownKey(B)) return 0.0;
    if (!horses.count(A) || !horses.count(B)) return 0.0;

    // symmetric
    PairKey key = (A < B) ? PairKey{ A, B } : PairKey{ B, A };

    // 1) LRU / RocksDB だけ使う
    {
        std::string dbkey = kK(key.a, key.b);
        double v;
        if (lruGet(dbkey, v)) {
            return v;
        }
        if (dbGetDouble(dbkey, v)) {
            // RocksDB 命中した分も LRU にだけ積む
            lruPut(dbkey, v);
            return v;
        }
    }

    // recursion guard
    std::string guardKey = key.a + "|" + key.b;
    if (kin_stack.count(guardKey)) return 0.0;
    kin_stack.insert(guardKey);

    double res = 0.0;

    if (A == B) {
        res = 0.5 * (1.0 + getInbreeding(A));
    }
    else {
        // --- depth-based expansion (always expand the deeper generation side) ---
        std::string u = A, v = B;
        int du = getDepth(u);
        int dv = getDepth(v);
        bool ku = hasKnownParentsInDB(u);
        bool kv = hasKnownParentsInDB(v);

        auto needSwap = [&]() -> bool {
            if (du < dv) return true;          // expand deeper side
            if (du > dv) return false;
            if (!ku && kv) return true;        // if same depth, prefer side that has parents
            if (ku && !kv) return false;
            return (u > v);                    // tie-breaker: lexical (stable & symmetric)
            };

        if (needSwap()) {
            std::swap(u, v);
            std::swap(du, dv);
            std::swap(ku, kv);
        }

        if (!hasKnownParentsInDB(u)) {
            res = 0.0;
        }
        else {
            const Horse& hu = horses.at(u);
            res = 0.5 * (getKinship(hu.Sire, v) + getKinship(hu.Dam, v));
        }
    }

    kin_stack.erase(guardKey);

    // RocksDB + LRU にだけ保存
    std::string dbkey = kK(key.a, key.b);
    lruPut(dbkey, res);
    dbPutDouble(dbkey, res);

    return res;
}

//=========================================================
//  Decomposition: enumerate paths and sum terms
//=========================================================
struct PathInfo {
    std::string ancestor;
    int depth = 0;
    std::vector<std::string> nodes; // start -> ... -> ancestor
};

static inline std::string joinPath(const std::vector<std::string>& v) {
    std::ostringstream oss;
    for (size_t i = 0; i < v.size(); i++) {
        if (i) oss << ">";
        oss << v[i];
    }
    return oss.str();
}

static inline bool independentPair(const PathInfo& p1, const PathInfo& p2) {
    const std::string& anc = p1.ancestor;
    std::unordered_set<std::string> s;
    s.reserve(p1.nodes.size() * 2);
    for (auto const& x : p1.nodes) {
        if (x == anc) continue;
        s.insert(x);
    }
    for (auto const& y : p2.nodes) {
        if (y == anc) continue;
        if (s.count(y)) return false;
    }
    return true;
}

static void dfsPaths(
    const std::string& cur,
    int depth,
    int maxDepth,
    std::vector<std::string>& stack,
    std::vector<PathInfo>& out,
    bool& aborted
) {
    if (aborted) return;
    if (depth >= maxDepth) return;
    if (out.size() >= MAX_PATHS_EACH) { aborted = true; return; }

    auto itH = horses.find(cur);
    if (itH == horses.end()) return;
    const Horse& h = itH->second;

    auto tryParent = [&](const std::string& p) {
        if (aborted) return;
        if (isUnknownKey(p) || !horses.count(p)) return;
        if (std::find(stack.begin(), stack.end(), p) != stack.end()) return;

        stack.push_back(p);

        PathInfo pi;
        pi.ancestor = p;
        pi.depth = depth + 1;
        pi.nodes = stack;
        out.push_back(std::move(pi));

        dfsPaths(p, depth + 1, maxDepth, stack, out, aborted);
        stack.pop_back();
        };

    tryParent(h.Sire);
    tryParent(h.Dam);
}

static void enumerateAllPathsToAncestors(
    const std::string& start,
    int maxDepth,
    std::vector<PathInfo>& paths,
    bool& aborted
) {
    paths.clear();
    aborted = false;
    if (isUnknownKey(start) || !horses.count(start)) return;

    // ★ 追加：start 自身を「祖先(depth=0)」として入れる
    {
        PathInfo self;
        self.ancestor = start;
        self.depth = 0;
        self.nodes = { start };
        paths.push_back(std::move(self));
    }

    std::vector<std::string> stack;
    stack.reserve(maxDepth + 2);
    stack.push_back(start);
    dfsPaths(start, 0, maxDepth, stack, paths, aborted);
}


struct InbreedTerm {
    std::string child;
    std::string sire;
    std::string dam;
    std::string ancestor;
    int nSire = 0;
    int nDam = 0;
    double contribF = 0.0;
    std::string pathSire;
    std::string pathDam;
};

static void computeInbreedingTerms(
    const std::string& childPk,
    int maxDepth,
    std::vector<InbreedTerm>& terms,
    std::unordered_map<std::string, double>& sumByAncestor,
    double& sumF,
    bool& aborted
) {
    terms.clear();
    sumByAncestor.clear();
    sumF = 0.0;
    aborted = false;

    if (!horses.count(childPk)) return;
    const Horse& c = horses.at(childPk);
    if (isUnknownKey(c.Sire) || isUnknownKey(c.Dam)) return;
    if (!horses.count(c.Sire) || !horses.count(c.Dam)) return;

    std::vector<PathInfo> ps, pd;
    bool ab1 = false, ab2 = false;
    enumerateAllPathsToAncestors(c.Sire, maxDepth, ps, ab1);
    enumerateAllPathsToAncestors(c.Dam, maxDepth, pd, ab2);
    aborted = ab1 || ab2;

    std::unordered_map<std::string, std::vector<size_t>> gs, gd;
    gs.reserve(ps.size() * 2);
    gd.reserve(pd.size() * 2);
    for (size_t i = 0; i < ps.size(); i++) gs[ps[i].ancestor].push_back(i);
    for (size_t i = 0; i < pd.size(); i++) gd[pd[i].ancestor].push_back(i);

    for (auto const& kv : gs) {
        const std::string& anc = kv.first;
        auto it = gd.find(anc);
        if (it == gd.end()) continue;

        double FA = getInbreeding(anc);
        double factorFA = 1.0 + FA;

        auto const& vi = kv.second;
        auto const& vj = it->second;

        for (size_t ii : vi) {
            for (size_t jj : vj) {
                const PathInfo& p1 = ps[ii];
                const PathInfo& p2 = pd[jj];

                if (!independentPair(p1, p2)) continue;

                int n1 = p1.depth;
                int n2 = p2.depth;

                double pow2 = std::pow(0.5, (double)(n1 + n2 + 1));
                double contrib = pow2 * factorFA;

                InbreedTerm t;
                t.child = childPk;
                t.sire = c.Sire;
                t.dam = c.Dam;
                t.ancestor = anc;
                t.nSire = n1;
                t.nDam = n2;
                t.contribF = contrib;
                t.pathSire = joinPath(p1.nodes);
                t.pathDam = joinPath(p2.nodes);

                terms.push_back(std::move(t));
                sumByAncestor[anc] += contrib;
                sumF += contrib;
            }
        }
    }
}

//=========================================================
//  Saving
//=========================================================
void saveInbreedingList(const std::string& filename,
    const std::vector<std::string>& targets)
{
    std::ofstream ofs(filename);
    if (!ofs) {
        std::cerr << "cannot open " << filename << "\n";
        return;
    }

    ofs << std::fixed << std::setprecision(12);

    // ここで input CSV 由来の情報をできるだけ出す
    // PrimaryKey, HorseName, YearStr, YearInt, Sire, Dam, F, Fpct(%) など
    ofs
        << "PrimaryKey,"
        << "HorseName,"
        << "YearStr,"
        << "SirePK,"
        << "DamPK,"
        << "F,"
        << "FPercentTrunc5\n";

    size_t idx = 0, total = targets.size();
    for (const auto& pk : targets) {
        idx++;

        double F = getInbreeding(pk);
        double pct = toPercentTrunc5(F);  // さっき定義してるやつそのまま利用

        // Horse 情報を拾う（存在しない PK だった場合でも落ちないように）
        const Horse* hptr = nullptr;
        auto it = horses.find(pk);
        if (it != horses.end()) {
            hptr = &it->second;
        }

        std::string horseName = keyToDisplayName.count(pk)
            ? keyToDisplayName[pk]
            : (hptr ? hptr->HorseName : pk);

            std::string yearStr = (hptr ? hptr->YearStr : "");
            std::string sirePk = (hptr ? hptr->Sire : "");
            std::string damPk = (hptr ? hptr->Dam : "");

            std::cout << "[F] (" << idx << "/" << total << ") "
                << horseName << " "
                << "[calc: " << std::fixed << std::setprecision(5)
                << pct << "%]\n";

            ofs
                << pk << ","                                          // PrimaryKey
                << csvEscape(horseName) << ","                        // HorseName（表示名）
                << csvEscape(yearStr) << ","                          // YearStr (文字列)
                << sirePk << ","                                      // SirePK
                << damPk << ","                                       // DamPK
                << std::setprecision(12) << F << ","                  // F(0-1)
                << std::setprecision(5) << pct                       // F%
                << "\n";
    }

    std::cout << "[done] " << filename << "\n";
}


void saveKinshipMatrix(const std::string& filename,
    const std::vector<std::string>& rows,
    const std::vector<std::string>& cols)
{
    if (rows.empty() || cols.empty()) return;

    std::ofstream ofs(filename);
    if (!ofs) { std::cerr << "cannot open " << filename << "\n"; return; }

    ofs << std::fixed << std::setprecision(8);

    ofs << "HorseName";
    for (const auto& c : cols) ofs << "," << csvEscape(keyToDisplayName[c]);
    ofs << "\n";

    size_t idx = 0, total = rows.size();
    for (const auto& r : rows) {
        idx++;
        std::cout << "[R] (" << idx << "/" << total << ") "
            << keyToDisplayName[r] << "\n";

        ofs << csvEscape(keyToDisplayName[r]);

        for (const auto& c : cols) {
            double v = getKinship(r, c);
            if (std::fabs(v) < 1e-12) v = 0.0;
            ofs << "," << std::setprecision(8) << v;
        }
        ofs << "\n";
    }

    std::cout << "[Matrix] " << filename << " 出力完了\n";
}
static void computeInbreedingTermsStreaming(
    const std::string& childPk,
    int maxDepth,
    std::ostream& ofsT, // 直接 f4 に吐く
    std::unordered_map<std::string, double>& sumByAncestor,
    std::unordered_map<std::string, int>& cntByAncestor,
    double& sumF,
    bool& abortedPaths,
    bool& abortedTerms,
    size_t maxTermsToWrite = 2'000'000  // ★安全弁：多すぎたら打ち切り
) {
    sumByAncestor.clear();
    cntByAncestor.clear();
    sumF = 0.0;
    abortedPaths = false;
    abortedTerms = false;

    if (!horses.count(childPk)) return;
    const Horse& c = horses.at(childPk);
    if (isUnknownKey(c.Sire) || isUnknownKey(c.Dam)) return;
    if (!horses.count(c.Sire) || !horses.count(c.Dam)) return;

    std::vector<PathInfo> ps, pd;
    bool ab1 = false, ab2 = false;
    enumerateAllPathsToAncestors(c.Sire, maxDepth, ps, ab1);
    enumerateAllPathsToAncestors(c.Dam, maxDepth, pd, ab2);
    abortedPaths = ab1 || ab2;

    std::unordered_map<std::string, std::vector<size_t>> gs, gd;
    gs.reserve(ps.size() * 2);
    gd.reserve(pd.size() * 2);
    for (size_t i = 0; i < ps.size(); i++) gs[ps[i].ancestor].push_back(i);
    for (size_t i = 0; i < pd.size(); i++) gd[pd[i].ancestor].push_back(i);

    const std::string childName = keyToDisplayName.count(childPk) ? keyToDisplayName[childPk] : childPk;

    size_t written = 0;

    for (auto const& kv : gs) {
        const std::string& anc = kv.first;
        auto it = gd.find(anc);
        if (it == gd.end()) continue;

        const double FA = getInbreeding(anc);
        const double factorFA = 1.0 + FA;

        auto const& vi = kv.second;
        auto const& vj = it->second;

        const std::string ancName = keyToDisplayName.count(anc) ? keyToDisplayName[anc] : anc;

        for (size_t ii : vi) {
            for (size_t jj : vj) {
                const PathInfo& p1 = ps[ii];
                const PathInfo& p2 = pd[jj];

                if (!independentPair(p1, p2)) continue;

                const int n1 = p1.depth;
                const int n2 = p2.depth;

                const double contrib = std::pow(0.5, double(n1 + n2 + 1)) * factorFA;

                sumByAncestor[anc] += contrib;
                cntByAncestor[anc] += 1;
                sumF += contrib;

                // ★term 行の書き出し（多すぎると落ちるので安全弁）
                if (!abortedTerms) {
                    if (written >= maxTermsToWrite) {
                        abortedTerms = true;
                        continue;
                    }
                    const double pct = toPercentTrunc5(contrib);

                    ofsT << csvEscape(childName) << "," << childPk << ","
                        << c.Sire << "," << c.Dam << ","
                        << csvEscape(ancName) << "," << anc << ","
                        << n1 << "," << n2 << ","
                        << contrib << "," << pct << ","
                        << csvEscape(joinPath(p1.nodes)) << ","
                        << csvEscape(joinPath(p2.nodes)) << "\n";

                    written++;
                }
            }
        }
    }
}


static void computeInbreedingSumOnly(
    const std::string& childPk,
    int maxDepth,
    std::unordered_map<std::string, double>& sumByAncestor,
    std::unordered_map<std::string, int>& cntByAncestor,
    double& sumF,
    bool& abortedPaths
) {
    sumByAncestor.clear();
    cntByAncestor.clear();
    sumF = 0.0;
    abortedPaths = false;

    if (!horses.count(childPk)) return;
    const Horse& c = horses.at(childPk);
    if (isUnknownKey(c.Sire) || isUnknownKey(c.Dam)) return;
    if (!horses.count(c.Sire) || !horses.count(c.Dam)) return;

    std::vector<PathInfo> ps, pd;
    bool ab1 = false, ab2 = false;
    enumerateAllPathsToAncestors(c.Sire, maxDepth, ps, ab1);
    enumerateAllPathsToAncestors(c.Dam, maxDepth, pd, ab2);
    abortedPaths = ab1 || ab2;

    std::unordered_map<std::string, std::vector<size_t>> gs, gd;
    gs.reserve(ps.size() * 2);
    gd.reserve(pd.size() * 2);
    for (size_t i = 0; i < ps.size(); i++) gs[ps[i].ancestor].push_back(i);
    for (size_t i = 0; i < pd.size(); i++) gd[pd[i].ancestor].push_back(i);

    for (auto const& kv : gs) {
        const std::string& anc = kv.first;
        auto it = gd.find(anc);
        if (it == gd.end()) continue;

        const double FA = getInbreeding(anc);
        const double factorFA = 1.0 + FA;

        auto const& vi = kv.second;
        auto const& vj = it->second;

        for (size_t ii : vi) {
            for (size_t jj : vj) {
                const PathInfo& p1 = ps[ii];
                const PathInfo& p2 = pd[jj];
                if (!independentPair(p1, p2)) continue;

                const int n1 = p1.depth;
                const int n2 = p2.depth;

                const double contrib = std::pow(0.5, double(n1 + n2 + 1)) * factorFA;

                sumByAncestor[anc] += contrib;
                cntByAncestor[anc] += 1;
                sumF += contrib;
            }
        }
    }
}
using ContribMap = std::unordered_map<std::string, double>;

static inline void addScaled(ContribMap& dst, const ContribMap& src, double scale) {
    for (auto const& kv : src) dst[kv.first] += kv.second * scale;
}
struct PairKey2 { std::string a, b; };
struct PairHash2 {
    size_t operator()(PairKey2 const& k) const noexcept {
        static std::hash<std::string> h;
        size_t x = h(k.a), y = h(k.b);
        x ^= (y + 0x9e3779b97f4a7c15ULL + (x << 6) + (x >> 2));
        return x;
    }
};
struct PairEq2 { bool operator()(PairKey2 const& x, PairKey2 const& y) const noexcept { return x.a == y.a && x.b == y.b; } };

static std::unordered_map<PairKey2, ContribMap, PairHash2, PairEq2> kmap_cache;
static thread_local std::unordered_set<std::string> kmap_stack;

static ContribMap getKinshipContribMap(const std::string& A, const std::string& B) {
    ContribMap empty;
    if (isUnknownKey(A) || isUnknownKey(B)) return empty;
    if (!horses.count(A) || !horses.count(B)) return empty;

    PairKey2 key = (A < B) ? PairKey2{ A,B } : PairKey2{ B,A };
    if (auto it = kmap_cache.find(key); it != kmap_cache.end()) return it->second;

    std::string guard = key.a + "|" + key.b;
    if (kmap_stack.count(guard)) return empty; // cycle safety
    kmap_stack.insert(guard);

    ContribMap res;

    if (A == B) {
        // f(A,A) = 0.5*(1+F(A)) = 0.5 + 0.5*F(A)
        res[A] += 0.5;
        if (hasKnownParentsInDB(A)) {
            const auto& h = horses.at(A);
            auto mFA = getKinshipContribMap(h.Sire, h.Dam); // = F(A) の分解
            addScaled(res, mFA, 0.5);
        }
    }
    else {
        // getKinship と同じ “深い側を展開” ルール
        std::string u = A, v = B;
        int du = getDepth(u), dv = getDepth(v);
        bool ku = hasKnownParentsInDB(u), kv = hasKnownParentsInDB(v);

        auto needSwap = [&]() -> bool {
            if (du < dv) return true;
            if (du > dv) return false;
            if (!ku && kv) return true;
            if (ku && !kv) return false;
            return (u > v);
            };
        if (needSwap()) { std::swap(u, v); }

        if (hasKnownParentsInDB(u)) {
            const auto& hu = horses.at(u);
            auto m1 = getKinshipContribMap(hu.Sire, v);
            auto m2 = getKinshipContribMap(hu.Dam, v);
            addScaled(res, m1, 0.5);
            addScaled(res, m2, 0.5);
        } // else res empty (0)
    }

    kmap_stack.erase(guard);
    kmap_cache[key] = res;
    return res;
}
static void computeInbreedingSumOnly_FAST(
    const std::string& childPk,
    std::unordered_map<std::string, double>& sumByAncestor,
    double& sumF
) {
    sumByAncestor.clear();
    sumF = 0.0;

    if (!horses.count(childPk)) return;
    const auto& c = horses.at(childPk);
    if (isUnknownKey(c.Sire) || isUnknownKey(c.Dam)) return;

    auto m = getKinshipContribMap(c.Sire, c.Dam); // これが欲しかった“祖先別寄与”
    sumByAncestor = std::move(m);
    for (auto const& kv : sumByAncestor) sumF += kv.second; // = F(child) と一致するはず
}
//=========================================================
//  (NEW) Decomposition without path-enumeration (DAG weight propagation)
//    - Computes exact decomposition of kinship f(Sire, Dam)
//    - sumByAncestor[anc] : contribution to F(child) from "meeting at anc"
//    - No ps/pd vectors, no MAX_PATHS_EACH. Memory stays bounded.
//=========================================================

static inline PairKey canonPair(const std::string& x, const std::string& y) {
    return (x < y) ? PairKey{ x, y } : PairKey{ y, x };
}

static inline bool isValidNode(const std::string& pk) {
    return !isUnknownKey(pk) && horses.count(pk);
}

static inline void addPending(
    std::unordered_map<PairKey, double, PairHash, PairEq>& pending,
    std::unordered_set<PairKey, PairHash, PairEq>& inQ,
    std::deque<PairKey>& q,
    const std::string& a,
    const std::string& b,
    double w
) {
    if (w == 0.0) return;
    if (!isValidNode(a) || !isValidNode(b)) return;

    PairKey k = canonPair(a, b);
    pending[k] += w;
    if (!inQ.count(k)) {
        inQ.insert(k);
        q.push_back(k);
    }
}

// Compute sumByAncestor for F(child)=f(sire,dam) without path enumeration.
static void computeInbreedingSumOnly_DAG(
    const std::string& childPk,
    std::unordered_map<std::string, double>& sumByAncestor,
    double& sumF,
    bool& aborted,
    size_t maxStates = 200'000'000   // safety cap (pairs processed)
) {
    sumByAncestor.clear();
    sumF = 0.0;
    aborted = false;

    if (!horses.count(childPk)) return;
    const Horse& c = horses.at(childPk);
    if (!isValidNode(c.Sire) || !isValidNode(c.Dam)) return;

    // pending weight per pair (A,B) (canonical: A<=B)
    std::unordered_map<PairKey, double, PairHash, PairEq> pending;
    pending.reserve(1 << 20);

    std::unordered_set<PairKey, PairHash, PairEq> inQ;
    inQ.reserve(1 << 20);

    std::deque<PairKey> q;
    addPending(pending, inQ, q, c.Sire, c.Dam, 1.0);

    size_t processed = 0;

    while (!q.empty()) {
        PairKey key = q.front();
        q.pop_front();
        inQ.erase(key);

        auto itW = pending.find(key);
        if (itW == pending.end()) continue;

        double w = itW->second;
        pending.erase(itW);
        if (w == 0.0) continue;

        if (++processed > maxStates) {
            aborted = true;
            break;
        }

        const std::string& A = key.a;
        const std::string& B = key.b;

        // invalid => contributes 0
        if (!isValidNode(A) || !isValidNode(B)) continue;

        // base: A==B => f(A,A)=0.5*(1+F(A))
        if (A == B) {
            // the "+1" part => contributes directly to "meeting at A"
            double add = 0.5 * w;
            sumByAncestor[A] += add;
            sumF += add;

            // the "+F(A)" part => 0.5*w*F(A) = 0.5*w*f(sire(A),dam(A))
            if (hasKnownParentsInDB(A)) {
                const Horse& h = horses.at(A);
                addPending(pending, inQ, q, h.Sire, h.Dam, 0.5 * w);
            }
            continue;
        }

        // general: f(A,B) = 0.5*( f(par(A).sire, B) + f(par(A).dam, B) )  (expand deeper side)
        std::string u = A, v = B;
        int du = getDepth(u);
        int dv = getDepth(v);
        bool ku = hasKnownParentsInDB(u);
        bool kv = hasKnownParentsInDB(v);

        auto needSwap = [&]() -> bool {
            if (du < dv) return true;
            if (du > dv) return false;
            if (!ku && kv) return true;
            if (ku && !kv) return false;
            return (u > v);
            };

        if (needSwap()) {
            std::swap(u, v);
            std::swap(du, dv);
            std::swap(ku, kv);
        }

        if (!hasKnownParentsInDB(u)) {
            // matches your getKinship(): res=0 when the chosen expand side has no parents
            continue;
        }

        const Horse& hu = horses.at(u);
        addPending(pending, inQ, q, hu.Sire, v, 0.5 * w);
        addPending(pending, inQ, q, hu.Dam, v, 0.5 * w);
    }
}
//=========================================================
//  (PATCH) saveInbreedingTermsSumOnly : use DAG version + remove pct/count columns
//=========================================================
void saveInbreedingTermsSumOnly_DAG(
    const std::string& filenameSum,
    const std::vector<std::string>& targets,
    int /*maxDepth*/   // ← 使わないけど呼び出し側を崩さないため残す
) {
    std::ofstream ofsS(filenameSum);
    if (!ofsS) { std::cerr << "cannot open " << filenameSum << "\n"; return; }

    ofsS << std::fixed << std::setprecision(12);
    // ★ 余計な「Pct」「TermCount」を消す
    ofsS << "ChildHorse,ChildPK,CommonAncestor,CommonAncestorPK,SumContributionF\n";

    size_t idx = 0, total = targets.size();

    for (auto const& child : targets) {
        idx++;
        std::cout << "[Sum] (" << idx << "/" << total << ") " << keyToDisplayName[child] << "\n";

        std::unordered_map<std::string, double> sumByAnc;
        double sumF = 0.0;
        bool aborted = false;

        computeInbreedingSumOnly_DAG(child, sumByAnc, sumF, aborted);

        if (aborted) {
            std::cout << "  [warn] DAG propagation aborted (maxStates hit)\n";
        }

        std::vector<std::pair<std::string, double>> agg(sumByAnc.begin(), sumByAnc.end());
        std::sort(agg.begin(), agg.end(), [](auto const& a, auto const& b) {
            if (a.second != b.second) return a.second > b.second;
            return a.first < b.first;
            });

        const std::string childName = keyToDisplayName.count(child) ? keyToDisplayName[child] : child;

        for (auto const& p : agg) {
            const std::string& anc = p.first;
            const double sum = p.second;
            const std::string ancName = keyToDisplayName.count(anc) ? keyToDisplayName[anc] : anc;

            ofsS << csvEscape(childName) << "," << child << ","
                << csvEscape(ancName) << "," << anc << ","
                << sum << "\n";
        }

        ofsS.flush();

        double Frec = getInbreeding(child);
        double diff = std::fabs(Frec - sumF);
        std::cout << "  F(rec)=" << std::setprecision(12) << Frec
            << "  F(sum)=" << std::setprecision(12) << sumF
            << "  diff=" << std::setprecision(12) << diff
            << "\n";
    }

    std::cout << "[done] " << filenameSum << "\n";
}

void saveInbreedingTermsSumOnly(
    const std::string& filenameSum,
    const std::vector<std::string>& targets,
    int maxDepth
) {
    std::ofstream ofsS(filenameSum);
    if (!ofsS) { std::cerr << "cannot open " << filenameSum << "\n"; return; }

    ofsS << std::fixed << std::setprecision(12);
    //ofsS << "ChildHorse,ChildPK,CommonAncestor,CommonAncestorPK,SumContributionF,SumContributionPct,TermCount\n";
    ofsS << "ChildHorse,ChildPK,CommonAncestor,CommonAncestorPK,SumContributionF\n";

    size_t idx = 0, total = targets.size();

    for (auto const& child : targets) {
        idx++;
        std::cout << "[Sum] (" << idx << "/" << total << ") " << keyToDisplayName[child] << "\n";

        std::unordered_map<std::string, double> sumByAnc;
        std::unordered_map<std::string, int> cntByAnc;
        double sumF = 0.0;
        bool abortedPaths = false;


        //computeInbreedingSumOnly(child, maxDepthForChild(child), sumByAnc, cntByAnc, sumF, abortedPaths);
		computeInbreedingSumOnly_FAST(child, sumByAnc, sumF); // 高速版で再計算（整合性チェックも兼ねる）

        if (abortedPaths) {
            std::cout << "  [warn] path enumeration aborted (MAX_PATHS_EACH hit)\n";
        }

        std::vector<std::pair<std::string, double>> agg(sumByAnc.begin(), sumByAnc.end());
        std::sort(agg.begin(), agg.end(), [](auto const& a, auto const& b) {
            if (a.second != b.second) return a.second > b.second;
            return a.first < b.first;
            });

        for (auto const& p : agg) {
            const std::string& anc = p.first;
            const double sum = p.second;
            const int cnt = cntByAnc[anc];

            const std::string childName = keyToDisplayName.count(child) ? keyToDisplayName[child] : child;
            const std::string ancName = keyToDisplayName.count(anc) ? keyToDisplayName[anc] : anc;

            ofsS << csvEscape(childName) << "," << child << ","
                << csvEscape(ancName) << "," << anc << ","
                << sum << "\n";
        }

        // 子ごとにflush（クラッシュ時の被害を減らす）
        ofsS.flush();

        double Frec = getInbreeding(child);
        double diff = std::fabs(Frec - sumF);
        std::cout << "  F(rec)=" << std::setprecision(12) << Frec
            << "  F(sum)=" << std::setprecision(12) << sumF
            << "  diff=" << std::setprecision(12) << diff
            << "  (maxDepth=" << maxDepth << ")\n";
    }

    std::cout << "[done] " << filenameSum << "\n";
}


void saveInbreedingTermsFiles(
    const std::string& filenameTerms,
    const std::string& filenameSum,
    const std::vector<std::string>& targets,
    int maxDepth
) {
    std::ofstream ofsT(filenameTerms);
    std::ofstream ofsS(filenameSum);
    if (!ofsT) { std::cerr << "cannot open " << filenameTerms << "\n"; return; }
    if (!ofsS) { std::cerr << "cannot open " << filenameSum << "\n"; return; }

    ofsT << std::fixed << std::setprecision(12);
    ofsS << std::fixed << std::setprecision(12);

    ofsT << "ChildHorse,ChildPK,SirePK,DamPK,CommonAncestor,CommonAncestorPK,nSire,nDam,ContributionF,ContributionPct,PathSire,PathDam\n";
    ofsS << "ChildHorse,ChildPK,CommonAncestor,CommonAncestorPK,SumContributionF,SumContributionPct,TermCount\n";

    size_t idx = 0, total = targets.size();

    for (auto const& child : targets) {
        idx++;
        std::cout << "[Term] (" << idx << "/" << total << ") " << keyToDisplayName[child] << "\n";

        std::unordered_map<std::string, double> sumByAnc;
        std::unordered_map<std::string, int> cntByAnc;
        double sumF = 0.0;
        bool abortedPaths = false;
        bool abortedTerms = false;

        computeInbreedingTermsStreaming(child, maxDepth, ofsT, sumByAnc, cntByAnc, sumF,
            abortedPaths, abortedTerms, 2'000'000);

        double Frec = getInbreeding(child);

        if (abortedPaths) {
            std::cout << "  [warn] path enumeration aborted (MAX_PATHS_EACH hit)\n";
        }
        if (abortedTerms) {
            std::cout << "  [warn] terms output aborted (too many terms; capped)\n";
        }

        std::vector<std::pair<std::string, double>> agg(sumByAnc.begin(), sumByAnc.end());
        std::sort(agg.begin(), agg.end(), [](auto const& a, auto const& b) {
            if (a.second != b.second) return a.second > b.second;
            return a.first < b.first;
            });

        for (auto const& p : agg) {
            const std::string& anc = p.first;
            double sum = p.second;
            int cnt = cntByAnc[anc];
            std::string childName = keyToDisplayName[child];
            std::string ancName = keyToDisplayName.count(anc) ? keyToDisplayName[anc] : anc;
            double pct = toPercentTrunc5(sum);

            ofsS << csvEscape(childName) << "," << child << ","
                << csvEscape(ancName) << "," << anc << ","
                << sum << "," << pct << "," << cnt << "\n";
        }

        // ★クラッシュ耐性：子ごとに flush
        ofsT.flush();
        ofsS.flush();

        double diff = std::fabs(Frec - sumF);
        std::cout << "  F(rec)=" << std::setprecision(12) << Frec
            << "  F(sum)=" << std::setprecision(12) << sumF
            << "  diff=" << std::setprecision(12) << diff << "\n";
    }

    std::cout << "[done] " << filenameTerms << "\n";
    std::cout << "[done] " << filenameSum << "\n";
}

//=========================================================
// main
//=========================================================
int main() {
    loadBloodlineCSV("D:\\AI\\C++\\input\\bloodline.csv");
    openDB();

    std::cout << "対象馬 (年/年レンジ/PrimaryKey をカンマ区切り): ";
    std::string raw;
    std::getline(std::cin, raw);

    std::unordered_set<std::string> targetSet;
    std::vector<std::string> idTokens;
    idTokens.reserve(16);

    // allow spaces in input
    std::regex reRange(R"(^\s*(\d{4})\s*-\s*(\d{4})\s*$)");
    std::regex reYear(R"(^\s*(\d{4})\s*$)");

    std::stringstream ss(raw);
    for (std::string tok; std::getline(ss, tok, ','); ) {
        tok = trim(tok);
        if (tok.empty()) continue;

        idTokens.push_back(tok);

        std::smatch m;
        if (std::regex_match(tok, m, reRange)) {
            int y1 = std::stoi(m[1]);
            int y2 = std::stoi(m[2]);
            if (y1 > y2) std::swap(y1, y2);

            for (const auto& kv : horses) {
                int y = kv.second.YearInt;
                if (y == INT_MIN) continue;
                if (y >= y1 && y <= y2) targetSet.insert(kv.first);
            }
        }
        else if (std::regex_match(tok, m, reYear)) {
            int y = std::stoi(m[1]);
            for (const auto& kv : horses) {
                if (kv.second.YearInt == y) targetSet.insert(kv.first);
            }
        }
        else {
            if (horses.count(tok)) targetSet.insert(tok);
            else std::cerr << "PrimaryKey \"" << tok << "\" not found - skip\n";
        }
    }

    std::cout << "[main] " << targetSet.size() << " targets found\n";
    if (targetSet.empty()) {
        std::cerr << "対象馬が 0 頭でした。\n";
        return 1;
    }

    std::vector<std::string> targetPks(targetSet.begin(), targetSet.end());
    std::sort(targetPks.begin(), targetPks.end());

    std::vector<std::string> allKeys;
    allKeys.reserve(horses.size());
    for (const auto& kv : horses) allKeys.push_back(kv.first);

    std::sort(allKeys.begin(), allKeys.end(),
        [&](const std::string& a, const std::string& b) {
            int ya = horses[a].YearInt;
            int yb = horses[b].YearInt;
            if (ya == yb) return a < b;
            return ya < yb;
        });

    std::string idLabel;
    for (size_t i = 0; i < idTokens.size(); i++) {
        if (i) idLabel += "_";
        idLabel += idTokens[i];
    }
    idLabel = safeLabel(idLabel);

    std::string f1 = "D:/AI/C++/out/inbreeding_of_" + idLabel + ".csv";
    std::string f2 = "D:/AI/C++/out/rel_all_to_" + idLabel + ".csv";
    std::string f3 = "D:/AI/C++/out/rel_" + idLabel + "_to_all.csv";
    std::string f4 = "D:/AI/C++/out/inbreeding_terms_of_" + idLabel + ".csv";
    std::string f5 = "D:/AI/C++/out/inbreeding_terms_sum_of_" + idLabel + ".csv";

    saveInbreedingList(f1, targetPks);
    // 行列出力の前
    lru.clear();
    order.clear();

    saveKinshipMatrix(f2, allKeys, targetPks);

    // 必要ならまた clear
    lru.clear();
    order.clear();
    //saveKinshipMatrix(f3, targetPks, allKeys);
    //saveInbreedingTermsFiles(f4, f5, targetPks, MAX_PATH_DEPTH);
    int globalDepth = 0;
    for (auto& pk : targetPks) {
        globalDepth = std::max(globalDepth, maxDepthForChild(pk));
    }

    saveInbreedingTermsSumOnly_DAG(f5, targetPks, globalDepth);



    // flush & close
    if (db) {
        db->Flush(rocksdb::FlushOptions());
        db.reset();
    }

    std::cout << "[main] 完了\n";
    return 0;
}
