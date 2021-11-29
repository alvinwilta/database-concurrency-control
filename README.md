# Tugas besar 2 Manajemen Basis Data

## Anggota Kelompok

13519039 Rayhan Alghifari Fauzta

13519087 Hizkia Raditya Pratama Roosadi

13519111 Febriawan Ghally Ar Rahman

13519163 Alvin Wilta

13519179 Akifa Nabil Ufairah


## Petunjuk Penggunaan Program Simulasi Simple Locking Protokol
1. Buka terminal atau command prompt pada folder `database-concurrency-control`
2. Jalankan  `python simpleLocking.py`
3. Masukkan input operasi, mulai dari yang operasi paling awal hingga yang paling baru, sesuai panduan pada program. Penerimaan input akan berhenti setelah semua transaksi telah menerima input untuk commit.
4. Pilih algoritma yang akan digunakan dengan memasukkan input `1` untuk menggunakan simple locking protocol (exclusive-only) dan `2` untuk menggunakan two-phase locking protocol (With exclusive and shared lock)
5. Output akan diterima berdasarkan urutan proses yang akan dilakukan oleh program concurrency control tergantung pilihan algoritma. Algoritma diimplementasikan dengan menggunakan sistem wound-wait untuk deadlock prevention.
