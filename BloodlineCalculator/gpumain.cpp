#include <iostream>
#include <unordered_map>
#include <vector>
#include <fstream>
#include <sstream>
#include <cuda_runtime.h>

using namespace std;

// 馬の血統データを格納する構造体
struct Horse {
    string PrimaryKey;
    string Sire; // 父
    string Dam;  // 母
};

// 血統データ（CPU側）
unordered_map<string, Horse> horses;
vector<string> horseNames;
vector<double> bloodCache;

// GPUメモリ用ポインタ
double* d_bloodCache;

// CSVファイルから血統データを読み込む
void loadCSV(const string& filename) {
    ifstream file(filename);
    if (!file.is_open()) {
        cerr << "エラー: ファイル " << filename << " を開けませんでした。" << endl;
        exit(EXIT_FAILURE);
    }

    string line;
    getline(file, line); // ヘッダーをスキップ

    while (getline(file, line)) {
        stringstream ss(line);
        string PrimaryKey, Sire, Dam;
        getline(ss, PrimaryKey, ',');
        getline(ss, Sire, ',');
        getline(ss, Dam, ',');

        horses[PrimaryKey] = { PrimaryKey, Sire, Dam };
        horseNames.push_back(PrimaryKey);
    }
    file.close();
    cout << "血統データ読み込み完了。馬の数: " << horses.size() << endl;
}

// GPUカーネル（並列計算）
__global__ void computeBloodPercentageKernel(double* d_bloodCache, int numHorses) {
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < numHorses) {
        for (int j = 0; j < numHorses; ++j) {
            // GPU側で血量計算（シンプルな例）
            d_bloodCache[i * numHorses + j] = (i == j) ? 1.0 : 0.5; // 仮の値
        }
    }
}

// GPUで血量計算を実行
void computeBloodPercentageGPU(int numHorses) {
    size_t size = numHorses * numHorses * sizeof(double);

    // GPUメモリを確保
    cudaMalloc((void**)&d_bloodCache, size);
    cudaMemset(d_bloodCache, 0, size);

    // CUDAカーネルを実行
    int threadsPerBlock = 256;
    int blocksPerGrid = (numHorses + threadsPerBlock - 1) / threadsPerBlock;
    computeBloodPercentageKernel << <blocksPerGrid, threadsPerBlock >> > (d_bloodCache, numHorses);

    // 計算結果を取得
    cudaMemcpy(bloodCache.data(), d_bloodCache, size, cudaMemcpyDeviceToHost);

    // GPUメモリを解放
    cudaFree(d_bloodCache);
}

// CSVファイルに血量データを書き出す
void saveCSV(const string& filename, int numHorses) {
    ofstream file(filename);
    if (!file.is_open()) {
        cerr << "エラー: ファイル " << filename << " を開けませんでした。" << endl;
        exit(EXIT_FAILURE);
    }

    // ヘッダー
    file << ",";
    for (const auto& name : horseNames) {
        file << name << ",";
    }
    file << "\n";

    // 各馬の血量データ
    for (int i = 0; i < numHorses; ++i) {
        file << horseNames[i] << ",";
        for (int j = 0; j < numHorses; ++j) {
            file << bloodCache[i * numHorses + j] << ",";
        }
        file << "\n";
    }

    file.close();
    cout << "血量データを " << filename << " に保存しました。" << endl;
}

int main() {
    string inputFile = "bloodline.csv";  // 入力ファイル
    string outputFile = "blood_percentage.csv"; // 出力ファイル

    cout << "CSVデータをロード開始..." << endl;
    loadCSV(inputFile);
    int numHorses = horseNames.size();
    bloodCache.resize(numHorses * numHorses);

    cout << "GPUで血量計算開始..." << endl;
    computeBloodPercentageGPU(numHorses);
    cout << "GPUでの血量計算完了！" << endl;

    cout << "計算結果をCSVに保存..." << endl;
    saveCSV(outputFile, numHorses);

    cout << "血量計算完了。結果を " << outputFile << " に保存しました。" << endl;
    return 0;
}
