/*
  Warnings:

  - A unique constraint covering the columns `[semantic_key,mail_label]` on the table `system_email_rule` will be added. If there are existing duplicate values, this will fail.
  - A unique constraint covering the columns `[semantic_key,mail_label]` on the table `user_email_rule` will be added. If there are existing duplicate values, this will fail.

*/
-- CreateIndex
CREATE UNIQUE INDEX "system_email_rule_semantic_key_mail_label_key" ON "system_email_rule"("semantic_key", "mail_label");

-- CreateIndex
CREATE UNIQUE INDEX "user_email_rule_semantic_key_mail_label_key" ON "user_email_rule"("semantic_key", "mail_label");
