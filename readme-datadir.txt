Data Repository README

This directory contains archive files of WNM (WIS Notification Message) collected from the JMA WIS2 development environment.
Two types of files are stored here:


---

1. Global Cache Data

Filename format: devgcHH.tar.gz

Description:

HH indicates the UTC hour (00–24).

Each file contains WNM obtained during the corresponding one-hour period from the Global Cache (operational server called dev).




---

2. Japan Node Data

Filename format: devnodeHH.tar.gz

Description:

HH indicates the UTC hour (00–24).

Each file contains WNM obtained during the corresponding one-hour period from the Japan Node.




---

Notes

Each .tar.gz file is an hourly bundle of WNM messages.

File sizes vary depending on the hour.

Empty or missing files may occur due to retrieval errors or missing publications.

The uncompressed .tar files are accumulated within each UTC hour (from HH:00 to HH:59). Their contents are not guaranteed; retrieving multiple times within the same hour may yield different results. The compressed archives (devgcHH.tar.gz / devnodeHH.tar.gz) represent a snapshot taken at the time of compression.

Each .tar.gz file is overwritten with the next day’s data 24 hours after creation.
