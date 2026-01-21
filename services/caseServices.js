
export async function createCase(client, data) {
  const res = await client.query(
    `
    INSERT INTO "Puma_L1_AI".cases
    (channel, intent_type, confidence_score, risk_flag, status, assigned_to)
    VALUES ($1,$2,$3,$4,$5,$6)
    RETURNING case_id
    `,
    [
      "email",
      data.intent,
      data.confidence,
      data.risk,
      data.status,
      data.owner,
    ]
  );
  return res.rows[0].case_id;
}
