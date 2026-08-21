CREATE OR REPLACE TABLE `database-sigma.Testing.fact_video_performa_daily` AS

WITH production_dedup AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id_konten
                   ORDER BY tanggal DESC
               ) AS rn
        FROM `database-sigma.Testing.silver_tt_video_production`
    )
    WHERE rn = 1
)

SELECT
    -- =============================
    -- VIDEO PERFORMANCE (FACT)
    -- =============================
    a.id_video,
    a.tanggal,
    a.toko,
    a.nama_kreator,
    a.informasi_video,
    a.waktu,
    a.produk AS produk_sku,
    a.vv AS views,
    a.klik_video_ke_live,
    a.produk_dilihat,
    a.klik_produk,
    a.pembeli,
    a.pesanan_video,
    a.produk_terjual_video,
    a.gmv_bruto_video,
    a.gmv_didapat_video,

    -- =============================
    -- VIDEO PRODUCTION (DIM ATTR)
    -- =============================
    b.tanggal AS tanggal_produksi,
    b.scripter,
    b.produk,
    b.editor,
    b.jenis_konten,
    b.tipe_konten,
    b.talent_visual,
    b.kategori_konten,
    b.isu,
    b.layout,
    b.script,
    b.cta,
    b.talent_vo,
    b.referensi_musik,
    b.sound,
    b.visual_hook,
    b.audio_hook,
    b.link_konten,
    b.hook

FROM `database-sigma.Testing.silver_tt_video` a
LEFT JOIN production_dedup b
    ON a.id_video = b.id_konten;