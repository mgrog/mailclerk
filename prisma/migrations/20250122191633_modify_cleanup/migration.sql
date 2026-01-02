/*
  Warnings:

  - You are about to drop the column `category` on the `auto_cleanup_setting` table. All the data in the column will be lost.
  - A unique constraint covering the columns `[mail_label,user_id]` on the table `auto_cleanup_setting` will be added. If there are existing duplicate values, this will fail.
  - Added the required column `mail_label` to the `auto_cleanup_setting` table without a default value. This is not possible if the table is not empty.

*/
-- DropIndex
DROP INDEX "auto_cleanup_setting_category_user_id_key";

-- AlterTable
ALTER TABLE "auto_cleanup_setting" DROP COLUMN "category",
ADD COLUMN     "mail_label" VARCHAR NOT NULL;

-- CreateIndex
CREATE UNIQUE INDEX "auto_cleanup_setting_mail_label_user_id_key" ON "auto_cleanup_setting"("mail_label", "user_id");
