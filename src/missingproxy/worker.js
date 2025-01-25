export default {
    async fetch(request, env) {
        if (!validate(request, env)) {
            return new Response(JSON.stringify({ error: "Unauthorized access" }), {
                status: 403,
                headers: { "Content-Type": "application/json" }
            });
        }
        console.log("HTTP 요청으로 데이터 갱신 시작");
        const result = await process(env);
        return new Response(JSON.stringify(result), { headers: { "Content-Type": "application/json" } });
    },

    async scheduled(event, env) {
        console.log("⏰ Scheduled Trigger");
        await process(env);
    }
};

async function process(env) {
    console.log("시작: 실종자 데이터 처리");
    const allRecords = await fetchMissingPersonsData(env);
    console.log("API 데이터 수집 완료", allRecords.length);

    const existingData = await getExistingDatabaseData(env);
    console.log("기존 DB 데이터 가져오기 완료", existingData.length);

    let { newRecords, deleteRowIDs, insertedNames, deletedNames } = await processRecords(allRecords, existingData);
    console.log(`새로운 데이터 개수: ${newRecords.length}, 삭제할 데이터 개수: ${deleteRowIDs.length}`);

    // 4. Kakao API를 newRecords에 적용하여 좌표 보강
    await enrichWithCoordinates(newRecords);

    // 5. DB 업데이트 실행 (삭제 후 삽입)
    await updateDatabase(env, newRecords, deleteRowIDs);

    console.log("✅ 데이터 처리 완료");
    return {
        message: "Database updated",
        new_records_names: insertedNames,
        deleted_records_names: deletedNames
    };
}

function validate(request, env) {
    const authHeader = request.headers.get("Authorization");
    if (!authHeader) return false;

    const expectedSecret = `Bearer ${env.WORKER_SECRET_PASSWORD}`;
    return authHeader === expectedSecret;
}


/**
 * 1. API에서 모든 실종자 데이터를 가져옴
 */
async function fetchMissingPersonsData(env) {
    const apiUrl = "https://www.safe182.go.kr/api/lcm/findChildList.do";
    const headers = { "Content-Type": "application/x-www-form-urlencoded" };
    const defaultParams = { esntlId: env.POLICE_AUTH_ID, authKey: env.POLICE_AUTH_KEY, rowSize: "100" };

    let firstResponse = await fetch(apiUrl, {
        method: "POST",
        headers: headers,
        body: new URLSearchParams(defaultParams).toString(),
    });

    let textData = await firstResponse.text();

    try {
        let firstData = JSON.parse(textData);
        if (!firstData.list) throw new Error("Invalid API response: missing 'list' field.");
        let totalCount = parseInt(firstData.totalCount, 10);
        let totalPages = Math.ceil(totalCount / 100);

        let allRecords = firstData.list || [];

        for (let page = 2; page <= totalPages; page++) {
            let params = new URLSearchParams({ ...defaultParams, page: page.toString() });

            let response = await fetch(apiUrl, {
                method: "POST",
                headers: headers,
                body: params.toString(),
            });

            let pageText = await response.text();
            let pageData;
            try {
                pageData = JSON.parse(pageText);
            } catch (error) {
                console.error(`Error parsing JSON from page ${page}:`, pageText);
                continue;
            }

            if (pageData.list) allRecords.push(...pageData.list);
        }

        return allRecords;
    } catch (error) {
        console.error("API response is not valid JSON:", textData);
        throw new Error("API returned an invalid response");
    }
}

/**
 * 2. DB에서 현재 저장된 모든 실종자의 ID, 해시값, 이름을 가져옴
 */
async function getExistingDatabaseData(env) {
    const existingRows = await env.DB.prepare("SELECT id, name, data_hash FROM missing_persons").all();
    return new Map(existingRows.results.map(row => [row.id, { hash: row.data_hash, name: row.name }]));
}

/**
 * 3. 데이터 비교 후 newRecords와 deleteRowIDs 얻기
 */
async function processRecords(allRecords, existingData) {
    let newRecords = [];
    let deleteRowIDs = [];
    let insertedNames = [];
    let deletedNames = [];

    async function computeHash(data) {
        const encoder = new TextEncoder();
        const filteredData = {
            name: data.nm,
            current_age: data.ageNow,
            age_when_missing: data.age,
            incident_date: data.occrde,
            clothing_description: data.alldressingDscd,
            person_type: data.writngTrgetDscd,
            gender: data.sexdstnDscd,
            additional_features: data.etcSpfeatr,
            photo_base64: data.tknphotoFile
        };

        const dataBuffer = encoder.encode(JSON.stringify(filteredData));
        const hashBuffer = await crypto.subtle.digest("SHA-256", dataBuffer);
        return Array.from(new Uint8Array(hashBuffer)).map(b => b.toString(16).padStart(2, "0")).join("");
    }

    let seenIds = new Set();

    for (const record of allRecords) {
        const id = record.msspsnIdntfccd;
        seenIds.add(id);

        const newHash = await computeHash(record);
        const existingRecord = existingData.get(id);

        if (!existingRecord) {
            newRecords.push({ ...record, data_hash: newHash });
            insertedNames.push(record.nm);
        } else if (existingRecord.hash !== newHash) {
            deleteRowIDs.push(id);
            newRecords.push({ ...record, data_hash: newHash });
            insertedNames.push(record.nm);
        }
    }

    for (const [existingId, record] of existingData.entries()) {
        if (!seenIds.has(existingId)) {
            deleteRowIDs.push(existingId);
            deletedNames.push(record.name);
        }
    }

    return { newRecords, deleteRowIDs, insertedNames, deletedNames };
}

/**
 * 4. Kakao API를 newRecords에만 적용
 */
async function enrichWithCoordinates(records, env) {
    const KAKAO_API_KEY = `KakaoAK ${env.KAKAO_REST_API_KEY}`;
    const DEFAULT_X = 126.9764;
    const DEFAULT_Y = 37.5867;

    async function fetchCoordinates(location) {
        if (!location || location.trim() === "") return { x: DEFAULT_X, y: DEFAULT_Y };

        try {
            let url = `https://dapi.kakao.com/v2/local/search/address.json?query=${encodeURIComponent(location.trim())}`;
            let response = await fetch(url, { headers: { Authorization: KAKAO_API_KEY } });
            let data = await response.json();

            if (data.documents && data.documents.length > 0) {
                return { x: parseFloat(data.documents[0].x), y: parseFloat(data.documents[0].y) };
            }
        } catch (error) {
            console.error(`Error fetching coordinates for ${location}:`, error);
        }
        return { x: DEFAULT_X, y: DEFAULT_Y };
    }

    for (const record of records) {
        const coords = await fetchCoordinates(record.occrAdres);
        record.incident_location = coords.address_name;
        record.incident_x = coords.x;
        record.incident_y = coords.y;
    }
}

/**
 * 5. DB 업데이트 (삭제 후 삽입)
 */
function escapeSQL(value) {
    if (value === null || value === undefined || value.trim() === '') return "NULL";
    return `'${value.replace(/'/g, "''").replace(/\n/g, ' ').replace(/\r/g, '').replace(/\t/g, ' ')}'`;
}

async function updateDatabase(env, newRecords, deleteRowIDs) {
    const BATCH_SIZE = 3;

    // Batch DELETE 처리
    for (let i = 0; i < deleteRowIDs.length; i += BATCH_SIZE) {
        let batch = deleteRowIDs.slice(i, i + BATCH_SIZE);
        let placeholders = batch.map(() => "?").join(", ");
        let deleteStmt = `DELETE FROM missing_persons WHERE id IN (${placeholders})`;
        await env.DB.prepare(deleteStmt).bind(...batch).run();
    }

    // Batch INSERT 처리
    for (let i = 0; i < newRecords.length; i += BATCH_SIZE) {
        let batch = newRecords.slice(i, i + BATCH_SIZE);

        let insertStmt = `
        INSERT INTO missing_persons (id, name, current_age, age_when_missing, incident_date, clothing_description, person_type, gender, incident_location, incident_x, incident_y, additional_features, photo_base64, data_hash)
        VALUES ${batch.map(() => "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").join(", ")}
        `;

        let insertValues = batch.flatMap(record => [
            record.msspsnIdntfccd,
            escapeSQL(record.nm),
            record.ageNow,
            record.age,
            escapeSQL(record.occrde),
            escapeSQL(record.alldressingDscd),
            escapeSQL(record.writngTrgetDscd),
            escapeSQL(record.sexdstnDscd),
            escapeSQL(record.occrAdres),
            record.incident_x,
            record.incident_y,
            escapeSQL(record.etcSpfeatr),
            record.tknphotoFile ? escapeSQL(record.tknphotoFile) : "NULL",
            record.data_hash
        ]);

        await env.DB.prepare(insertStmt).bind(...insertValues).run();
    }
}