export default {
  async fetch(request, env) {
    if (!validate(request, env)) {
      return new Response(JSON.stringify({ error: "Unauthorized access" }), {
        status: 403,
        headers: { "Content-Type": "application/json" },
      });
    }

    const url = new URL(request.url);
    const x = parseFloat(url.searchParams.get("x"));
    const y = parseFloat(url.searchParams.get("y"));
    const count_only = parseFloat(url.searchParams.get("count_only")) || false;
    const max_people = parseInt(url.searchParams.get("max_people")) || 10;
    const threshold_km = parseFloat(url.searchParams.get("threshold_km")) || 5;

    if (isNaN(x) || isNaN(y) || isNaN(max_people) || isNaN(threshold_km)) {
      return new Response(JSON.stringify({ error: "Invalid parameters" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }

    // Haversine 알고리즘
    const HAVERSINE_DISTANCE_SQL = `
            SELECT id, name, current_age, age_when_missing, incident_date, clothing_description, 
                   person_type, gender, incident_location, incident_x, incident_y, additional_features, 
                   photo_base64,
                   (6371 * ACOS(
                        COS(RADIANS(?)) * COS(RADIANS(incident_y)) * COS(RADIANS(incident_x) - RADIANS(?)) + 
                        SIN(RADIANS(?)) * SIN(RADIANS(incident_y))
                   )) AS distance
            FROM missing_persons
            WHERE (
                6371 * ACOS(
                    COS(RADIANS(?)) * COS(RADIANS(incident_y)) * COS(RADIANS(incident_x) - RADIANS(?)) + 
                    SIN(RADIANS(?)) * SIN(RADIANS(incident_y))
                )
            ) <= ?
            ORDER BY distance ASC
            LIMIT ?;
        `;

    // D1에서 반경 내 실종자 검색 실행
    const result = await env.DB.prepare(HAVERSINE_DISTANCE_SQL)
      .bind(y, x, y, y, x, y, threshold_km, max_people)
      .all();

    if (count_only) {
      return new Response(
        JSON.stringify({
          totalCount: result.results.length,
        }),
        { headers: { "Content-Type": "application/json" } }
      );
    }
    return new Response(
      JSON.stringify({
        totalCount: result.results.length,
        people: result.results,
      }),
      { headers: { "Content-Type": "application/json" } }
    );
  },
};

function validate(request, env) {
  const authHeader = request.headers.get("Authorization");
  if (!authHeader) return false;

  const expectedSecret = `Bearer ${env.WORKER_SECRET_PASSWORD}`;
  return authHeader === expectedSecret;
}
