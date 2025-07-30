const fs = require('fs');
const readline = require('readline');
const Records = require('./records.model');

const upload = async (req, res) => {
    const { file } = req;

    if (!file) {
        return res.status(400).json({ error: 'No se proporcionó ningún archivo' });
    }

    const startTime = Date.now();
    let processedRecords = 0;
    let errors = 0;
    const batchSize = 1000; // Procesar en lotes de 1000 registros
    let batch = [];
    let isFirstLine = true;

    try {
        // Crear stream para leer el archivo línea por línea
        const fileStream = fs.createReadStream(file.path);
        const rl = readline.createInterface({
            input: fileStream,
            crlfDelay: Infinity // Para manejar archivos con diferentes tipos de salto de línea
        });

        // Función para procesar un lote de registros
        const processBatch = async (records) => {
            if (records.length === 0) return;

            try {
                await Records.insertMany(records, { ordered: false });
                processedRecords += records.length;
            } catch (error) {
                // Si hay errores de duplicados u otros, intentar insertar uno por uno
                for (const record of records) {
                    try {
                        await Records.create(record);
                        processedRecords++;
                    } catch (err) {
                        errors++;
                        console.error('Error insertando registro:', err.message);
                    }
                }
            }
        };

        // Procesar cada línea del archivo
        for await (const line of rl) {
            // Saltar la primera línea si es un header
            if (isFirstLine) {
                isFirstLine = false;
                // Verificar si la primera línea parece ser un header
                if (line.toLowerCase().includes('firstname') || line.toLowerCase().includes('email')) {
                    continue;
                }
            }

            if (line.trim() === '') continue; // Saltar líneas vacías

            try {
                // Parsear la línea CSV con mejor manejo de comillas y comas dentro de campos
                const columns = [];
                let current = '';
                let inQuotes = false;
                
                for (let i = 0; i < line.length; i++) {
                    const char = line[i];
                    if (char === '"') {
                        inQuotes = !inQuotes;
                    } else if (char === ',' && !inQuotes) {
                        columns.push(current.trim());
                        current = '';
                    } else {
                        current += char;
                    }
                }
                columns.push(current.trim()); // Agregar el último campo

                // Validar que tenga el número correcto de columnas
                if (columns.length >= 6) {
                    const record = {
                        id: parseInt(columns[0]) || 0,
                        firstname: columns[1] || '',
                        lastname: columns[2] || '',
                        email: columns[3] || '',
                        email2: columns[4] || '',
                        profession: columns[5] || ''
                    };

                    batch.push(record);

                    // Procesar el lote cuando alcance el tamaño deseado
                    if (batch.length >= batchSize) {
                        await processBatch(batch);
                        batch = [];
                    }
                }
            } catch (parseError) {
                errors++;
                console.error('Error parseando línea:', parseError.message);
            }
        }

        // Procesar el último lote si queda algo
        if (batch.length > 0) {
            await processBatch(batch);
        }

        // Eliminar el archivo temporal
        fs.unlinkSync(file.path);

        const processingTime = Date.now() - startTime;

        return res.status(200).json({
            message: 'Archivo procesado exitosamente',
            stats: {
                recordsProcessed: processedRecords,
                errors: errors,
                processingTimeMs: processingTime,
                processingTimeSeconds: Math.round(processingTime / 1000 * 100) / 100
            }
        });

    } catch (error) {
        console.error('Error procesando archivo:', error);

        // Intentar eliminar el archivo temporal en caso de error
        try {
            if (fs.existsSync(file.path)) {
                fs.unlinkSync(file.path);
            }
        } catch (unlinkError) {
            console.error('Error eliminando archivo temporal:', unlinkError);
        }

        return res.status(500).json({
            error: 'Error procesando el archivo',
            details: error.message,
            stats: {
                recordsProcessed: processedRecords,
                errors: errors
            }
        });
    }
};

const list = async (_, res) => {
    try {
        const data = await Records
            .find({})
            .limit(10)
            .lean();

        return res.status(200).json(data);
    } catch (err) {
        return res.status(500).json(err);
    }
};

module.exports = {
    upload,
    list,
};
