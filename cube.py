import atoti as tt
import pandas as pd

df = pd.read_csv("cleaned_power_plants.csv")

for col in ["commissioning_year", "country", "capacity_mw"]:
    if col not in df.columns:
        raise Exception(f"Kolom '{col}' tidak ditemukan di data!")

print(df["commissioning_year"].value_counts(dropna=False))

df["commissioning_year"] = pd.to_numeric(df["commissioning_year"], errors="coerce")
df = df[df["commissioning_year"].notna() & (df["commissioning_year"] > 0)]

df["decade"] = (df["commissioning_year"] // 10) * 10
df = df.dropna(subset=["decade"])
df["decade"] = df["decade"].astype(int)  

assert df["decade"].isna().sum() == 0, "Masih ada NaN di kolom decade!"

df["umur"] = 2025 - df["commissioning_year"]
df["umur"] = df["umur"].where(df["umur"] > 0, 1)
df["capacity_per_year"] = df["capacity_mw"] / df["umur"]

print("Jumlah NaN di decade:", df["decade"].isna().sum())
print("Tipe data decade:", df["decade"].dtype)
print(df[df["decade"].isna()])

print(df.isna().sum())  

df = df.reset_index(drop=True)

for col in df.select_dtypes(include=["object"]).columns:
    df[col] = df[col].fillna("Unknown")  

session = tt.Session.start()

key_col = "gppd_idnr" if "gppd_idnr" in df.columns else "name"
store = session.read_pandas(df, keys=[key_col], table_name="power_plants")

cube = session.create_cube(store, "PowerPlantCube")
h = cube.hierarchies
m = cube.measures

h["Waktu"] = [store["decade"], store["commissioning_year"]]

h["Lokasi"] = [store["country"]]

m["Total Kapasitas"] = tt.agg.sum(store["capacity_mw"])

m["Kapasitas per Tahun"] = tt.agg.sum(store["capacity_per_year"])

print("Akses dashboard Atoti di URL berikut:")
print(session.url)

# 1. Negara mana dengan total kapasitas terbesar?
q1 = cube.query(m["Total Kapasitas"], levels={h["Lokasi"]: "country"})
print("1. Negara dengan total kapasitas terbesar:")
print(q1.sort_values("Total Kapasitas", ascending=False).head(1))

# 2. Total kapasitas semua pembangkit listrik?
q2 = cube.query(m["Total Kapasitas"])
print("\n2. Total kapasitas semua pembangkit:")
print(q2)

# 3. Negara mana dengan kapasitas per tahun terbesar?
q3 = cube.query(m["Kapasitas per Tahun"], levels={h["Lokasi"]: "country"})
print("\n3. Negara dengan kapasitas per tahun tertinggi:")
print(q3.sort_values("Kapasitas per Tahun", ascending=False).head(1))

# 4. Kapasitas rata-rata per tahun di seluruh dunia?
q4 = cube.query(m["Kapasitas per Tahun"])
print("\n4. Kapasitas rata-rata per tahun secara global:")
print(q4)

# 5. Bagaimana distribusi kapasitas berdasarkan dekade?
q5 = cube.query(m["Total Kapasitas"], levels={h["Waktu"]: "decade"})
print("\n5. Total kapasitas per dekade:")
print(q5)

# 6. Negara mana yang memiliki kapasitas tertinggi pada dekade 2010-an?
q6 = cube.query(m["Total Kapasitas"], levels={h["Waktu"]: "decade", h["Lokasi"]: "country"})
q6_2010s = q6[q6.index.get_level_values("decade") == 2010]
print("\n6. Negara dengan kapasitas tertinggi di dekade 2010-an:")
print(q6_2010s.sort_values("Total Kapasitas", ascending=False).head(1))