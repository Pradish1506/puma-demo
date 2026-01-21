export async function createChildCase(client, parentCaseId, type, team) {
  await client.query(
    `
    INSERT INTO "Puma_L1_AI".child_cases
    (parent_case_id, type, assigned_team, status)
    VALUES ($1,$2,$3,'open')
    `,
    [parentCaseId, type, team]
  );
}
