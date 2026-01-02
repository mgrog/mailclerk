/*
  Warnings:

  - You are about to drop the column `is_disabled` on the `auto_cleanup_setting` table. All the data in the column will be lost.

*/
-- AlterTable
ALTER TABLE "auto_cleanup_setting" DROP COLUMN "is_disabled";
