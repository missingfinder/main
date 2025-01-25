/**
 * 1. Create sql files
 * $ node init.js
 * 
 * 2. Upload init sql (free version)
 * $ for file in insert_missing_persons_*.sql; do
    yes | wrangler d1 execute missing_db --remote --file="$file"
    done
 */

import fs from "fs";
import dotenv from "dotenv";
dotenv.config();

const POLICE_AUTH_ID = process.env.POLICE_AUTH_ID
const POLICE_AUTH_KEY = process.env.POLICE_AUTH_KEY
const KAKAO_REST_API_KEY = process.env.KAKAO_REST_API_KEY
const SQL_BATCH_SIZE = 3;


async function main() {
    console.log("🚀 실행 시작...");

    // 1. API에서 실종자 데이터 가져오기
    const allRecords = await fetchMissingPersonsData();

    // 2. DB에서 현재 데이터 가져오기 (로컬 실행이므로 빈 Map 리턴)
    const existingData = new Map([]);

    // 3. newRecords와 deleteRowIDs 얻기
    let { newRecords, deleteRowIDs, insertedNames, deletedNames } = await processRecords(allRecords, existingData);

    // 4. Kakao API를 newRecords에만 적용
    await enrichWithCoordinates(newRecords);

    // 5. SQL 파일 여러 개 생성
    const sqlFiles = generateSQLFiles(newRecords, deleteRowIDs);

    // 6. SQL 파일 저장
    sqlFiles.forEach(({ fileName, sqlContent }) => {
        fs.writeFileSync(fileName, sqlContent, "utf8");
        console.log(`✅ SQL 파일 저장 완료: ${fileName}`);
    });

    console.log("✅ 모든 SQL 파일 생성 완료!");
}


/**
 * 1. API에서 모든 실종자 데이터를 가져옴
 */
async function fetchMissingPersonsData() {
    const apiUrl = "https://www.safe182.go.kr/api/lcm/findChildList.do";
    const headers = { "Content-Type": "application/x-www-form-urlencoded" };
    const defaultParams = { esntlId: POLICE_AUTH_ID, authKey: POLICE_AUTH_KEY, rowSize: "100" };

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
 * 3. newRecords와 deleteRowIDs 얻기
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
async function enrichWithCoordinates(records) {
    const KAKAO_API_KEY = `KakaoAK ${KAKAO_REST_API_KEY}`;
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
        record.incident_x = coords.x;
        record.incident_y = coords.y;
    }
}


// 5. SQL file generate to initialize
function generateSQLFiles(newRecords, deleteRowIDs) {
    let sqlFiles = [];
    let fileIndex = 1;

    if (deleteRowIDs.length > 0) {
        let deleteSQL = `DELETE FROM missing_persons WHERE id IN (${deleteRowIDs.join(", ")});`;
        sqlFiles.push({ fileName: `delete_missing_persons.sql`, sqlContent: deleteSQL });
    }

    let batchCount = Math.ceil(newRecords.length / SQL_BATCH_SIZE);

    console.log(`BATCH SIZE = ${batchCount}`);

    for (let i = 0; i < batchCount; i++) {
        let startIdx = i * SQL_BATCH_SIZE;
        let endIdx = startIdx + SQL_BATCH_SIZE;
        let batch = newRecords.slice(startIdx, endIdx);

        console.log(`i=${startIdx}, e=${endIdx}, Length: ${batch.length}`);

        if (batch.length === 0) continue;

        let insertValues = batch.map(record =>
            `(${record.msspsnIdntfccd}, '${record.nm.replace(/'/g, "''")}', ${record.ageNow}, ${record.age}, '${record.occrde}', '${record.alldressingDscd}', '${record.writngTrgetDscd}', '${record.sexdstnDscd}', '${record.occrAdres.replace(/'/g, "''")}', ${record.incident_x}, ${record.incident_y}, ${escapeSQL(record.etcSpfeatr)}, ${record.tknphotoFile ? `'${record.tknphotoFile}'` : "NULL"}, '${record.data_hash}')`
        ).join(",\n");

        let sqlContent = `
        INSERT INTO missing_persons (id, name, current_age, age_when_missing, incident_date, clothing_description, person_type, gender, incident_location, incident_x, incident_y, additional_features, photo_base64, data_hash)
        VALUES 
        ${insertValues};
        `.trim();

        let fileName = `insert_missing_persons_${fileIndex}.sql`;
        sqlFiles.push({ fileName, sqlContent });
        fileIndex++;
    }

    return sqlFiles;
}


function escapeSQL(value) {
    if (value === null || value === undefined || value.trim() === '') return "NULL";
    return `'${value.replace(/'/g, "''").replace(/\n/g, ' ').replace(/\r/g, '').replace(/\t/g, ' ')}'`;
}


main().catch(console.error);
