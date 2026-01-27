/*
  Warnings:

  - You are about to drop the column `initial_scan_completed` on the `user` table. All the data in the column will be lost.
  - You are about to drop the column `setup_completed` on the `user` table. All the data in the column will be lost.

*/
-- AlterTable
ALTER TABLE "user" DROP COLUMN "initial_scan_completed",
DROP COLUMN "setup_completed",
ADD COLUMN     "is_initial_scan_complete" BOOLEAN NOT NULL DEFAULT false,
ADD COLUMN     "is_setup_complete" BOOLEAN NOT NULL DEFAULT false;
