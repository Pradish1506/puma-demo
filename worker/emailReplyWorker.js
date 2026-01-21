import { pool } from "../db.js";

async function sendReplies() {
  const client = await pool.connect();

  const { rows } = await client.query(`
    SELECT *
    FROM "Puma_L1_AI".email_queue
    WHERE status='pending'
    LIMIT 20
  `);

  for (const mail of rows) {
    console.log(" Sending:", mail.payload);

    await client.query(
      `
      UPDATE "Puma_L1_AI".email_queue
      SET status='sent', sent_at=NOW()
      WHERE email_id=$1
      `,
      [mail.email_id]
    );
  }

  client.release();
}

sendReplies();
