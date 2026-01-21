export async function queueEmail(client, caseId, to, template) {
  await client.query(
    `
    INSERT INTO "Puma_L1_AI".email_queue
    (case_id, to_address, payload, status)
    VALUES ($1,$2,$3,'pending')
    `,
    [caseId, to, JSON.stringify({ template })]
  );
}
