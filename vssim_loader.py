# vssim_loader.py
# ===== VS Code / Jupyter 両対応：$PEDESTRIAN優先 + 無ければ24行スキップ =====
import argparse
from pathlib import Path
import pandas as pd
from io import StringIO
import sys
import os

# ====== 追加: セッション跨ぎで上書きできるデフォルトパス ======
DEFAULT_PATH = r"C:\Users\yonem\OneDrive - 学校法人立命館\デスクトップ\病院デフォルト\デフォルト (1).pp"

def set_default_path(path: str):
    global DEFAULT_PATH
    DEFAULT_PATH = str(path)

def _effective_default_path():
    return os.environ.get("VSSIM_DEFAULT_PATH", DEFAULT_PATH)

def load_table_autodetect(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"ファイルが見つかりません: {p}")

    enc_used, text = None, None
    for enc in ("utf-8", "cp932", "utf-8-sig"):
        try:
            text = p.read_text(encoding=enc)
            enc_used = enc
            break
        except Exception:
            continue
    if text is None:
        raise ValueError(f"ファイルを読み取れません（エンコーディング判定失敗）: {p}")

    lines = text.splitlines()

    ped_idx = None
    for i, ln in enumerate(lines):
        if ln.strip().upper().startswith("$PEDESTRIAN:"):
            ped_idx = i
            break

    def is_comment_or_blank(ln: str) -> bool:
        s = ln.strip()
        return (not s) or s.startswith("*")

    if ped_idx is not None:
        header_line = lines[ped_idx].strip()
        header_after_colon = header_line.split(":", 1)[1] if ":" in header_line else header_line

        data_lines = []
        for ln in lines[ped_idx + 1:]:
            if is_comment_or_blank(ln):
                continue
            if ln.strip().startswith("$"):
                break
            data_lines.append(ln)

        if not data_lines:
            raise ValueError("データ行が見つかりません（$PEDESTRIAN: の後に有効な行がありません）。")

        pseudo_csv = header_after_colon + "\n" + "\n".join(data_lines)
        buf = StringIO(pseudo_csv)
        try:
            df_ = pd.read_csv(buf, sep=";", engine="python", on_bad_lines="error")
            used_sep = ";"
        except Exception:
            buf.seek(0)
            df_ = pd.read_csv(buf, sep=r"[;\t,]+", engine="python", on_bad_lines="skip")
            used_sep = r"[;\t,]+"

        df_.columns = [c.strip() for c in df_.columns]

        rename_map = {}
        for c in df_.columns:
            if c.strip().upper() in {"$PEDESTRIAN:NO", "$PEDESTRIAN_NO", "PEDESTRIAN:NO"}:
                rename_map[c] = "PEDESTRIAN_NO"
        if rename_map:
            df_ = df_.rename(columns=rename_map)

        if "PEDESTRIAN_NO" not in df_.columns and "NO" in df_.columns:
            df_ = df_.rename(columns={"NO": "PEDESTRIAN_NO"})

        print(f"検出: encoding={enc_used}, section='$PEDESTRIAN', sep={repr(used_sep)}")
        return df_

    if len(lines) <= 24:
        raise ValueError("ファイルの行数が24行以下のため、削除後に読み込むデータがありません。")
    clean_lines = lines[24:]

    header_line = clean_lines[0]
    seps = [";", "\t", ","]
    sep = max(seps, key=lambda x: header_line.count(x))
    if header_line.count(sep) == 0:
        sep = r"\s+"

    print(f"[fallback] $PEDESTRIAN: なし -> encoding={enc_used}, skipped_first_lines=24, sep={repr(sep)}")
    buf = StringIO("\n".join(clean_lines))
    try:
        df_ = pd.read_csv(buf, sep=sep, engine="python", on_bad_lines="error")
    except Exception as e:
        print(f"[info] 一回目失敗、フォールバックします: {e}")
        buf.seek(0)
        df_ = pd.read_csv(buf, sep=r"[;\t,]+", engine="python", on_bad_lines="skip")

    df_.columns = [c.strip() for c in df_.columns]
    if "PEDESTRIAN_NO" not in df_.columns and "NO" in df_.columns:
        df_ = df_.rename(columns={"NO": "PEDESTRIAN_NO"})
    return df_

def compute_max_per_ped_and_median_by_route(df: pd.DataFrame):
    need_cols = ["PEDESTRIAN_NO", "DISTTRAVTOT", "STAROUTDECNO"]
    col_map = {}
    if any(c.upper() == "$PEDESTRIAN:NO" for c in df.columns):
        for c in df.columns:
            if c.upper() == "$PEDESTRIAN:NO":
                col_map[c] = "PEDESTRIAN_NO"
    if col_map:
        df = df.rename(columns=col_map)

    missing = [c for c in need_cols if c not in df.columns]
    if missing:
        raise KeyError(f"必要な列が見つかりません: {missing}. 取得列={list(df.columns)}")

    df["DISTTRAVTOT"] = pd.to_numeric(df["DISTTRAVTOT"], errors="coerce")
    try:
        df["PEDESTRIAN_NO"] = pd.to_numeric(df["PEDESTRIAN_NO"], errors="ignore")
    except Exception:
        pass

    df_valid = df.dropna(subset=["DISTTRAVTOT"]).copy()
    if df_valid.empty:
        raise ValueError("DISTTRAVTOT がすべて欠損でした。")

    idx = df_valid.groupby("PEDESTRIAN_NO")["DISTTRAVTOT"].idxmax()
    df_max_per_ped = df_valid.loc[idx].copy().reset_index(drop幸)
    # ↑ エディタで "reset_index(drop=True)" に直してください（もし文字化けしたら）

    df_max_per_ped = df_valid.loc[idx].copy().reset_index(drop=True)

    median_by_route = (
        df_max_per_ped
        .groupby("STAROUTDECNO", dropna=True)["DISTTRAVTOT"]
        .median()
        .reset_index(name="DISTTRAVTOT_median")
        .sort_values(["DISTTRAVTOT_median", "STAROUTDECNO"], ascending=[False, True])
        .reset_index(drop=True)
    )
    return df_max_per_ped, median_by_route

def main(path: str | None = None, save: str | None = None):
    used_path = path or _effective_default_path()
    df_raw = load_table_autodetect(used_path)
    df_max_per_ped, median_by_route = compute_max_per_ped_and_median_by_route(df_raw)

    print("\n=== 各歩行者で DISTTRAVTOT が最大の行（先頭5件） ===")
    print(df_max_per_ped.head())

    print("\n=== STAROUTDECNO ごとの DISTTRAVTOT 中央値 ===")
    print(median_by_route)

    if save:
        ext = Path(save).suffix.lower()
        if ext == ".xlsx":
            with pd.ExcelWriter(save, engine="openpyxl") as w:
                df_max_per_ped.to_excel(w, index=False, sheet_name="max_per_pedestrian")
                median_by_route.to_excel(w, index=False, sheet_name="median_by_route")
            print(f"\n✅ 保存しました（Excel 2シート）: {save}")
        elif ext == ".csv":
            median_by_route.to_csv(save, index=False, encoding="utf-8-sig")
            print(f"\n✅ 保存しました（CSV: STAROUTDECNO中央値テーブル）: {save}")
        else:
            base = Path(save)
            base_dir = base.parent if str(base.parent) not in ("", ".") else Path.cwd()
            out1 = base_dir / (base.stem + "_max_per_ped.csv")
            out2 = base_dir / (base.stem + "_median_by_route.csv")
            df_max_per_ped.to_csv(out1, index=False, encoding="utf-8-sig")
            median_by_route.to_csv(out2, index=False, encoding="utf-8-sig")
            print(f"\n✅ 保存しました: {out1}\n✅ 保存しました: {out2}")

    return df_raw, df_max_per_ped, median_by_route, used_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="VISSIM pp ローダー & 集計（歩行者最大距離→STAROUTDECNO中央値）")
    parser.add_argument("--path", type=str, default=None, help="入力ファイルパス（.pp / .txt / .csv）")
    parser.add_argument("--save", type=str, default=None, help="保存先パス（.xlsx 推奨。未指定なら保存しない）")

    # Jupyter の余計な引数を無視
    argv = sys.argv[1:]
    in_jupyter = ("ipykernel" in sys.modules) or ("JPY_PARENT_PID" in os.environ)
    if in_jupyter:
        args = parser.parse_args(args=[])
    else:
        args, _ = parser.parse_known_args(argv)

    try:
        _, _, _, used_path = main(path=args.path, save=args.save)
        print(f"\n[Done] file = {used_path}")
    except SystemExit:
        raise
    except Exception as e:
        print(f"[Error] {e}", file=sys.stderr)
        sys.exit(1)
