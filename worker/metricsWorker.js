import { pool } from "../db.js";

async function calcMetrics() {
  const client = await pool.connect();

  const fcr = await client.query(`
    SELECT COUNT(*) FILTER (WHERE assigned_to='ai')::float /
           NULLIF(COUNT(*),0) * 100 AS rate
    FROM "Puma_L1_AI".cases
  `);

  await client.query(
    `
    INSERT INTO "Puma_L1_AI".platform_metrics (fcr_rate)
    VALUES ($1)
    `,
    [fcr.rows[0].rate]
  );

  client.release();
}

calcMetrics();
