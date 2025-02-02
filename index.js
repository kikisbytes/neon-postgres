import pg from 'pg'
import { faker } from '@faker-js/faker';

const pool = new pg.Pool({
    connectionString: `postgresql://${process.env.DB_USERNAME}:${process.env.DB_PASSWORD}@ep-gentle-band-a5wwzdol.us-east-2.aws.neon.tech/test?sslmode=require`
});

async function createUsersTable() {
    const createTableQuery = `
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            firstName VARCHAR(100),
            lastName VARCHAR(100),
            name VARCHAR(200),
            email VARCHAR(200),
            ipAddress VARCHAR(45)
        );
    `;

    try {
        await pool.query(createTableQuery);
        console.log('Users table created or already exists');
    } catch (error) {
        console.error('Error creating table:', error);
        throw error;
    }
}

function generateUsersBatch(batchSize) {
    const users = [];
    for (let i = 0; i < batchSize; i++) {
        const firstName = faker.person.firstName();
        const lastName = faker.person.lastName();
        users.push([
            firstName,
            lastName,
            `${firstName} ${lastName}`,
            faker.internet.email({ firstName, lastName }),
            faker.internet.ip()
        ]);
    }
    return users;
}

async function insertUsersBatch(users) {
    const query = `
        INSERT INTO users (firstName, lastName, name, email, ipAddress)
        VALUES ${users.map((_, index) => `($${index * 5 + 1}, $${index * 5 + 2}, $${index * 5 + 3}, $${index * 5 + 4}, $${index * 5 + 5})`).join(',')}
    `;
    
    const values = users.flat();
    
    try {
        await pool.query(query, values);
    } catch (error) {
        console.error('Error inserting batch:', error);
        throw error;
    }
}

async function generateData(totalRecords, batchSize) {
    try {
        await createUsersTable();
        
        const totalBatches = Math.ceil(totalRecords / batchSize);
        let processedRecords = 0;
        
        console.log(`Starting to generate ${totalRecords} records in batches of ${batchSize}`);
        console.time('Data Generation');
        
        for (let i = 0; i < totalBatches; i++) {
            const currentBatchSize = Math.min(batchSize, totalRecords - processedRecords);
            const users = generateUsersBatch(currentBatchSize);
            await insertUsersBatch(users);
            
            processedRecords += currentBatchSize;
            
            if (i % 100 === 0 || processedRecords === totalRecords) {
                const progress = ((processedRecords / totalRecords) * 100).toFixed(2);
                console.log(`Progress: ${progress}% (${processedRecords.toLocaleString()} records)`);
            }
        }
        
        console.timeEnd('Data Generation');
        console.log(`Successfully generated ${processedRecords.toLocaleString()} records`);
        
    } catch (error) {
        console.error('Error in data generation:', error);
    } finally {
        await pool.end();
    }
}

const totalRecords = 10000000;
const batchSize = 1000;
generateData(totalRecords, batchSize).catch(console.error);
