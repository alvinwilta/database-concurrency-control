# Tugas Besar 2 Manajemen Basis Data

Program ini dibuat untuk memenuhi tugas besar 2 Manajemen Basis Data IF3140 yang mensimulasikan beberapa concurrency protocol antara lain:

1. Simple Locking (Exclusive Locks Only)
2. Serial Optimistic Concurrency Control (OCC)
3. Multiversion Timestamp Ordering Concurrency Control (MVCC)

## Anggota Kelompok

13519039 Rayhan Alghifari Fauzta

13519087 Hizkia Raditya Pratama Roosadi

13519111 Febriawan Ghally Ar Rahman

13519163 Alvin Wilta

13519179 Akifa Nabil Ufairah

## Requirements

- python 3

## Petunjuk Penggunaan Program Simulasi

### Simple Locking Protokol

1. Buka terminal atau command prompt pada folder `database-concurrency-control`
2. Jalankan `python simpleLocking.py` (atau `python3 simpleLocking.py`)
3. Masukkan input operasi, mulai dari yang operasi paling awal hingga yang paling baru, sesuai panduan pada program. Penerimaan input akan berhenti setelah semua transaksi telah menerima input untuk commit.
4. Pilih algoritma yang akan digunakan dengan memasukkan input `1` untuk menggunakan simple locking protocol (exclusive-only) dan `2` untuk menggunakan two-phase locking protocol (With exclusive and shared lock)
5. Output akan diterima berdasarkan urutan proses yang akan dilakukan oleh program concurrency control tergantung pilihan algoritma. Algoritma diimplementasikan dengan menggunakan sistem wound-wait untuk deadlock prevention.

### Serial Optimistic Concurrency Control

1. Buka terminal atau command prompt pada folder `database-concurrency-control`
2. Masukan input yang diinginkan untuk OCC pada file [input_occ.txt](input_occ.txt). Sesuaikan dengan format yang ada pada contoh.
3. Jalankan `python occ.py` (atau `python3 occ.py`)
4. Program akan otomatis berjalan secara _verbose_ atau menjabarkan tiap langkah dari awal hingga akhir sesuai dengan prinsip OCC.
5. Hasil schedule akhir akan ditampilkan setelah program selesai berjalan.

### Multiversion Timestamp Ordering Concurrency Control

1. Buka terminal atau command prompt pada folder `database-concurrency-control`
2. Jalankan `python mvcc.py` (atau `python3 mvcc.py`)
3. Masukkan input operasi, mulai dari yang operasi paling awal hingga yang paling baru, sesuai panduan pada program. Penerimaan input akan berhenti setelah semua transaksi telah menerima input untuk commit.
4. Masukkan input timestamp dari masing-masing transaksi yang sudah dimasukkan, ketik enter jika timestamp dari transaksi sama dengan id dari transaksi terkait
5. Output akan diterima dengan urutan berupa perubahan dan penambahan versi yang terjadi dalam protokol, kemudian diikuti dengan versi-versi yang telah dibuat dan hasil operasi akhir yang dilakukan dalam schedule ini.
