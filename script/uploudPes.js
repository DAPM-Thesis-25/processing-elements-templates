const axios = require("axios");
const prompt = require("prompt-sync")();
const jwt = require("jsonwebtoken");
const chalk = require("chalk");
const FormData = require("form-data");
const fs = require("fs");

 const api_url = "http://localhost:8081/";
 const api_url_b = "http://localhost:8082/";
//const api_url = "http://130.225.70.66:8081/";
//const api_url_b = "http://130.225.70.65:8082/";
// const api_url = "http://dapm3.compute.dtu.dk:8081/api/";
let token = "";
let token_b="";

// helper for step narration + error handling
const step = async (desc, fn) => {
    console.log(`${desc}`);
    try {
        const res = await fn();
        console.log(chalk.green("✔ Success\n"));
        return res;
    } catch (err) {
        console.error(`❌ Failed at step: "${desc}"`);
        if (err.response) {
            console.error(`Status: ${err.response.status}`);
            console.error(`Response: ${JSON.stringify(err.response.data)}`);
        } else {
            console.error(err.message);
        }
        process.exit(1);
    }
};

const scenario = async () => {
    // --- Login ---
    const loginRes = await step("Anna logs in", () =>
        axios.post(`${api_url}api/auth/authenticate`, {
            username: "anna",
            password: "dapm",
        })
    );
    console.log("Anna successfully logged in");

    token = loginRes.data.token;
    console.log(`Anna's token: ${chalk.green(token)}`);
    console.table(jwt.decode(token));

    const authHeaders = {
        headers: { Authorization: `Bearer ${token}` },
    };

    // --- Upload New Processing Element ---
    // await step("Anna uploads a new ProcessingElement", async () => {
    //     const formData = new FormData();
    //     formData.append("template", fs.createReadStream("../EventSource.java"));
    //     formData.append("configSchema", fs.createReadStream("../orga_eventsource_config_schema.json"));
    //     formData.append("tier", "FREE"); // optional
    //     formData.append("output", "Event"); // optional, comma-separated
    //     formData.append("processingElementType", "SOURCE"); // optional, comma-separated

    //     return axios.post(`${api_url}api/templates/uploadNewProcessingElement`, formData, {
    //         headers: {
    //             ...authHeaders.headers,
    //             ...formData.getHeaders(),
    //         },
    //     });
    // });

    await step("Anna uploads a new ProcessingElement", async () => {
        const formData = new FormData();
        formData.append("template", fs.createReadStream("../HospitalEventSource.java"));
        formData.append("configSchema", fs.createReadStream("../orga_hospitaleventsource_config_schema.json"));
        formData.append("tier", "FREE"); // optional
        formData.append("output", "Event"); // optional, comma-separated
        formData.append("processingElementType", "SOURCE"); // optional, comma-separated

        return axios.post(`${api_url}api/templates/uploadNewProcessingElement`, formData, {
            headers: {
                ...authHeaders.headers,
                ...formData.getHeaders(),
            },
        });
    });

    // await step("Anna uploads a new ProcessingElement", async () => {
    //     const formData = new FormData();
    //     formData.append("template", fs.createReadStream("../HospitalEventSource2.java"));
    //     formData.append("configSchema", fs.createReadStream("../orga_hospitaleventsource2_config_schema.json"));
    //     formData.append("tier", "FREE"); // optional
    //     formData.append("output", "Event"); // optional, comma-separated
    //     formData.append("processingElementType", "SOURCE"); // optional, comma-separated

    //     return axios.post(`${api_url}api/templates/uploadNewProcessingElement`, formData, {
    //         headers: {
    //             ...authHeaders.headers,
    //             ...formData.getHeaders(),
    //         },
    //     });
    // });


    // await step("Anna uploads a new ProcessingElement", async () => {
    //     const formData = new FormData();
    //     formData.append("template", fs.createReadStream("../LanguageFilter.java"));
    //     formData.append("configSchema", fs.createReadStream("../orga_languagefilter_config_schema.json"));
    //     formData.append("tier", "FREE"); // optional
    //     formData.append("output", "Event"); // optional
    //     formData.append("inputs", "Event"); // optional, comma-separated
    //     formData.append("processingElementType", "OPERATOR"); // optional, comma-separated
    //     return axios.post(`${api_url}api/templates/uploadNewProcessingElement`, formData, {
    //         headers: {
    //             ...authHeaders.headers,
    //             ...formData.getHeaders(),
    //         },
    //     });
    // });

    await step("Anna uploads a new ProcessingElement", async () => {
        const formData = new FormData();
        formData.append("template", fs.createReadStream("../DepartmentFilter.java"));
        formData.append("configSchema", fs.createReadStream("../orga_departmentfilter_config_schema.json"));
        formData.append("tier", "FREE"); // optional
        formData.append("output", "Event"); // optional
        formData.append("inputs", "Event"); // optional, comma-separated
        formData.append("processingElementType", "OPERATOR"); // optional, comma-separated
        return axios.post(`${api_url}api/templates/uploadNewProcessingElement`, formData, {
            headers: {
                ...authHeaders.headers,
                ...formData.getHeaders(),
            },
        });
    });

    await step("Anna uploads a new ProcessingElement", async () => {
        const formData = new FormData();
        formData.append("template", fs.createReadStream("../PetriNetSink.java"));
        formData.append("configSchema", fs.createReadStream("../orga_petrinetsink_config_schema.json"));
        formData.append("tier", "FREE"); // optional
        formData.append("inputs", "PetriNet"); // optional, comma-separated
        formData.append("processingElementType", "SINK"); // optional, comma-separated

        return axios.post(`${api_url}api/templates/uploadNewProcessingElement`, formData, {
            headers: {
                ...authHeaders.headers,
                ...formData.getHeaders(),
            },
        });
    });






     // --- Login in org B ---
    const loginRes2 = await step("Anna logs in org b", () =>
        axios.post(`${api_url_b}api/auth/authenticate`, {
            username: "anna",
            password: "dapm",
        })
    );
    console.log("Anna successfully logged in org b");

    token2 = loginRes2.data.token;
    console.log(`Anna's token: ${chalk.green(token2)}`);
    console.table(jwt.decode(token2));
    

    const authHeaders2 = {
        headers: { Authorization: `Bearer ${token2}` },
    };

    // --- Upload New Processing Element ---
    await step("Anna uploads a new ProcessingElement", async () => {
        const formData2 = new FormData();
        formData2.append("template", fs.createReadStream("../HeuristicsMiner.java"));
        formData2.append("configSchema", fs.createReadStream("../orgb_heuristicsminer_config_schema.json"));
        formData2.append("tier", "BASIC"); // optional
        formData2.append("output", "PetriNet"); // optional
        formData2.append("inputs", "Event"); // optional, comma-separated
        formData2.append("processingElementType", "OPERATOR"); // optional, comma-separated

        return axios.post(`${api_url_b}api/templates/uploadNewProcessingElement`, formData2, {
            headers: {
                ...authHeaders2.headers,
                ...formData2.getHeaders(),
            },
        });
    });
};

scenario();
