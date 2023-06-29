// The script checks the document structure defined in docs/structure.yml
// and also checks if the cross-references in the documents are valid
// (i.e. a cross-reference should not end up with '.md').
// To run the script, use the following command from the root directory:
// node .github\scripts\doc_structure_checker.js

const yaml = require('js-yaml');
const fs = require('fs'); // Required for reading the file

const OmniPaxosDocBasePath = 'docs'
const MaxNumberOfPropertiesInDocIndex = 2; // path, tags

const yamlData = fs.readFileSync(`${OmniPaxosDocBasePath}/structure.yml`, 'utf8');
const docStructureJsonData = yaml.load(yamlData, {json: true});

check_docs(docStructureJsonData)
console.log("Document structure is valid")

function check_docs(docStructure) {
    for (const title in docStructure) {
        const section = docStructure[title];
        if (section.hasOwnProperty('path')) {
            checkDocIndex(title, section)
            checkFilePath(`${OmniPaxosDocBasePath}/${section.path}`)
            checkCrossReferences(`${OmniPaxosDocBasePath}/${section.path}`)
        } else {
            check_docs(section);
        }
    }
}

function checkDocIndex(key, section) {
    if (section.hasOwnProperty("tags") && !Array.isArray(section.tags)) {
        throw new Error(`Invalid document structure: 'tags' property for section '${key}' must be an array`);
    }

    if (typeof section.path !== "string") {
        throw new Error(`Invalid document structure: 'path' property for section '${key}' must be a string`);
    }

    if (Object.keys(section).length > MaxNumberOfPropertiesInDocIndex) {
        throw new Error(`Invalid document structure: Extra properties found in section '${key}'`);
    }

    if (Object.keys(section).length === 2 && !section.hasOwnProperty("tags")) {
        throw new Error(`Invalid document structure: Unexpected properties found in section '${key}'`);
    }
}

function checkFilePath(filePath) {
    try {
        fs.accessSync(filePath, fs.constants.F_OK);
        return true; // File exists
    } catch (error) {
        throw new Error(`File "${filePath}" does not exist`);
    }
}

function checkCrossReferences(filePath) {
    const content = fs.readFileSync(filePath, 'utf8');
    const regex = /\[(.*?)\]\((.*?)(\.md)\)/g;
    const crossReferences = [];

    let match;
    while ((match = regex.exec(content)) !== null) {
        const reference = match[0];
        crossReferences.push(reference);
    }
    if (crossReferences.length !== 0) {
        throw new Error(`Invalid cross references found in file "${filePath}": "${crossReferences}"`);
    }
    return crossReferences;
}