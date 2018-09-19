import { uuid, sparqlEscapeString, sparqlEscapeDateTime, sparqlEscapeUri } from 'mu';
import { querySudo as query, updateSudo as update } from './auth-sudo';
import fs from 'fs-extra';
import archiver from 'archiver';
import xmlbuilder from 'xmlbuilder';

const filePath = process.env.FILE_PATH || '/data/files/';
const fileGraph = process.env.FILE_GRAPH || 'http://mu.semte.ch/graphs/public';
const STATUS_PROCESSING = "http://mu.semte.ch/vocabularies/ext/toezicht-status/PACKAGING";
const STATUS_PACKAGED = "http://mu.semte.ch/vocabularies/ext/toezicht-status/PACKAGED";
const STATUS_PACKAGING_FAILED = "http://mu.semte.ch/vocabularies/ext/toezicht-status/PACKAGING_FAILED";

/**
 * convert results of select query to an array of objects.
 * @method parseResult
 * @return {Array}
 */
const parseResult = function(result) {
  const bindingKeys = result.head.vars;
  return result.results.bindings.map((row) => {
    const obj = {};
    bindingKeys.forEach((key) => obj[key] = row[key] && row[key].value);
    return obj;
  });
};

/**
 * convert a file url (share://the/path/to/the/file) to the local path
 * e.g `filePath/the/path/to/the/file`
 * @method fileUrlToPath
 * @return {String}
 */
const fileUrlToPath = function(fileUrl) {
  return fileUrl.replace('share:\/\/', filePath);
};

const pathToFileUrl = function(path) {
  return path.replace(filePath, 'share://');
};

const generateZipFileName = function(dossier, zipUUID){
  const timestamp = new Date().toISOString().replace(/[.:]/g, '_');
  const decisionDate = dossier.besluitdatum ? dossier.besluitdatum.substr(0, 10).replace(/-/g, ''): '';
  const bestuur = `${dossier.classificatieNaam}_${dossier.naam}`.replace(/[^a-z0-9]/gi, '');
  const type = dossier.besluitTypeLabel.replace(/[^a-z0-9]/gi, '');
  return `Inzending_financieel_${bestuur}_${type}_${dossier.authenticityStatus || ''}_${dossier.boekjaar || ''}_${decisionDate}_${timestamp}_${zipUUID}.zip`;
};

/**
 * create zip file in packagePath with the provided name(.zip),
 * containing the provided files and metadata
 * @method createZipFile
 */
const createZipFile = async function(name, files, borderel, publicatie) {
  const filename = `${filePath}${name}`;
  var output = await fs.createWriteStream(filename);
  const archive = archiver('zip', {
    zlib: { level: 9 } // Sets the compression level.
  });
  // listen for all archive data to be written
  // 'close' event is fired only when a file descriptor is involved
  output.on('close', function() {
    console.log(`${filename} was created: ${archive.pointer()} bytes`);
  });
  // good practice to catch warnings (ie stat failures and other non-blocking errors)
  archive.on('warning', function(err) {
      throw err;
  });
  // good practice to catch this error explicitly
  archive.on('error', function(err) {
    throw err;
  });
  archive.pipe(output);
  files.map( (file) => {
    archive.file(fileUrlToPath(file.file), {name: file.filename});
  });
  archive.file(borderel, {name: 'Borderel.xml'}); // The capital really matters
  archive.file(publicatie, {name: 'Publicatie.xml'}); // The capital really matters
  await archive.finalize();
  await fs.unlink(borderel);
  await fs.unlink(publicatie);  
  return pathToFileUrl(filename);
};

/**
 * @method createBorderel
 */
const createBorderel = async function(dossier, files, publicatie) {
  // see https://github.com/oozcitak/xmlbuilder-js/wiki
  const xml = xmlbuilder.create('ns1:Borderel', {}, {}, {separateArrayItems: true})
          .att('xsi:schemaLocation', 'http://MFT-01-00.abb.vlaanderen.be/Borderel Borderel.xsd')
          .att('xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance')
          .att('xmlns:ns1', 'http://MFT-01-00.abb.vlaanderen.be/Borderel');
  const bestanden = files.map( (file => { return { Bestand: { Bestandsnaam: file.filename } }; }));
  if (publicatie)
    bestanden.push({ Bestand: { Bestandsnaam: 'Publicatie.xml' } }); // The capital really matters
  xml.ele({
    'ns1:Bestanden': bestanden,
    'ns1:RouteringsMetadata': {
      Entiteit:'ABB',
      Toepassing: 'DIGITAAL TOEZICHT',
      'ParameterSet': [
        {
          ParameterParameterWaarde: {
            Parameter: 'SLEUTEL',
            ParameterWaarde: dossier.kbonummer
          }
        },
        {
          ParameterParameterWaarde: {
            Parameter: 'FLOW',
            ParameterWaarde: 'PUBLICATIE'
          }
        }
      ]
    }
  }
  );
  const output = xml.end({pretty: true});
  const filename = `${filePath}${dossier.id}-borderel.xml`;
  await fs.writeFile(filename, output);
  return filename;
};

/**
 * @method createPublicatie
 */  
const createPublicatie = async function(dossier) {
  const xml = xmlbuilder.create('n1:PublicatieBeleidsrapport', {}, {}, {separateArrayItems: true})
         .att('xsi:schemaLocation', 'http://PUB_Beleidsrapport-01-00.abb.vlaanderen.be/Borderel/Publicatie.xsd')
          .att('xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance')
         .att('xmlns:n1', 'http://PUB_Beleidsrapport-01-00.abb.vlaanderen.be/Borderel');
  const parameterSet = [
    ['Ondernemingsnummer', dossier.kbonummer],
    ['MaatschappelijkeNaam', dossier.naam],
    ['TypeBestuur', dossier.classificatieNaam],
    ['RapportCode', dossier.besluitTypeLabel],
    ['Boekjaar', dossier.boekjaar || ''],
    ['Status', dossier.authenticityStatus || ''],
    ['DatumGoedkeuring', dossier.besluitdatum ? dossier.besluitdatum.substr(0, 10) : '']
  ];
  xml.ele({
    'n1:ParameterSet': parameterSet.map(function(param) {
      return {
        ParameterParameterWaarde: {
          Parameter: param[0],
          ParameterWaarde: param[1]
        }
      };
    })
  }); 
  const output = xml.end({pretty: true});
  const filename = `${filePath}${dossier.id}-publicatie.xml`;
  await fs.writeFile(filename, output);
  return filename;
};

/**
 * add package information to a Toezicht report
 * @method addPackage
 */
const addPackage = async function(report, packagePath, packageID, fileName, graph) {
  await update(`
       PREFIX toezicht: <http://mu.semte.ch/vocabularies/ext/supervision/>
       PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
       PREFIX mu:   <http://mu.semte.ch/vocabularies/core/>
       PREFIX dbpedia: <http://dbpedia.org/ontology/>
       PREFIX dcterms: <http://purl.org/dc/terms/>

       INSERT DATA {
         GRAPH <${graph}> {
             ${sparqlEscapeUri(report)} toezicht:package ${sparqlEscapeUri(packagePath)}.
         }
         GRAPH <${fileGraph}> {
             ${sparqlEscapeUri(packagePath)} a nfo:FileDataObject;
                                             nfo:fileName ${sparqlEscapeString(`${fileName}`)};
                                             dcterms:format "application/zip";
                                             dcterms:created ${sparqlEscapeDateTime(new Date())};
                                             mu:uuid ${sparqlEscapeString(packageID)};
                                             dbpedia:fileExtension "zip".
         }
       }
  `);
};

/**
 * update the internal status of a report
 * @method updateInternalDossierStatus
 */
const updateInternalDossierStatus = async function(report, status, graph) {
  await update(`
       PREFIX toezicht: <http://mu.semte.ch/vocabularies/ext/supervision/>
       PREFIX dcterms: <http://purl.org/dc/terms/>

       DELETE {
         GRAPH <${graph}> {
             ${sparqlEscapeUri(report)} dcterms:modified ?modified.
             ${sparqlEscapeUri(report)} toezicht:status ?status.
         }
       }
       WHERE {
         GRAPH <${graph}> {
             {
               ${sparqlEscapeUri(report)} dcterms:modified ?modified.
             }
             UNION
             {
               OPTIONAL{ ${sparqlEscapeUri(report)} toezicht:status ?status }
             }
         }
       }

       ;

       INSERT DATA {
         GRAPH <${graph}> {
             ${sparqlEscapeUri(report)} dcterms:modified ${sparqlEscapeDateTime(new Date())};
                                        toezicht:status ${sparqlEscapeUri(status)}.
         }
       }
  `);
};

/**
 * retrieve files linked to a dossier
 * @method fetchFilesForDossier
 * @param {IRI} dossierIri
 * @return {Array}
 */
const fetchFilesForDossier = async function(dossierIri, graph) {
  const result = await query(`
       PREFIX mu:   <http://mu.semte.ch/vocabularies/core/>
       PREFIX nie:     <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
       PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
       PREFIX dcterms: <http://purl.org/dc/terms/>
       PREFIX adms:    <http://www.w3.org/ns/adms#>
       PREFIX toezicht: <http://mu.semte.ch/vocabularies/ext/supervision/>

       SELECT ?file ?filename ?format ?size
       WHERE {
         GRAPH <${graph}> {
             ${sparqlEscapeUri(dossierIri)} a toezicht:InzendingVoorToezicht;
                                        nie:hasPart ?uploadFile.
         }

         GRAPH <${fileGraph}> {
             ?uploadFile nfo:fileName ?filename.
             ?file nie:dataSource ?uploadFile;
                   dcterms:format ?format;
                   nfo:fileSize ?size.
         }
       }
`);
  return parseResult(result);
};

/**
 * fetch reports in sent status that are not yet packaged
 * @method fetchReportsToBePackaged
 * @return {Array}
 */
const fetchFinancialDossiersToBePackaged = async function() {
  const result = await query(`
       PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
       PREFIX mu:   <http://mu.semte.ch/vocabularies/core/>
       PREFIX nie:     <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
       PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
       PREFIX dcterms: <http://purl.org/dc/terms/>
       PREFIX adms:    <http://www.w3.org/ns/adms#>
       PREFIX toezicht: <http://mu.semte.ch/vocabularies/ext/supervision/>
       PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
       PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>


       SELECT ?uri ?id ?graph ?besluitTypeLabel ?authenticityStatus ?boekjaar ?besluitdatum ?bestuurseenheid ?naam ?kbonummer ?classificatieNaam
       WHERE {

         GRAPH ?graph {
             ?uri a toezicht:InzendingVoorToezicht;
                     adms:status <http://data.lblod.info/document-statuses/verstuurd>;
                     toezicht:decisionType ?besluitType;
                     mu:uuid ?id;
                     dcterms:modified ?modified;
                     dcterms:subject ?bestuurseenheid.
             FILTER NOT EXISTS {
                ?uri toezicht:status ?status.
             }

            VALUES ?besluitType {
                 <http://data.lblod.info/DecisionType/80536574a0ec8ea88685510b713aa566a5f16cfd575fabd8f7943bccaaad00e4>
                 <http://data.lblod.info/DecisionType/d6e90eb6e3ceda4f9a47b214b3ab47274670d3621f34bf8984f4c7d99f97dcc2>
                 <http://data.lblod.info/DecisionType/26697366c439cac0fd35581416baffec2368d765d61888bfb4bafd22ddbc8b33>
             }

             OPTIONAL {
                 ?uri toezicht:authenticityType ?authenticityType .
                  GRAPH  <http://mu.semte.ch/graphs/public> {
                     ?authenticityType skos:prefLabel ?authenticityStatus .
                  }
             }
           
             OPTIONAL {
                 ?uri toezicht:temporalCoverage ?boekjaar .
             }
           
             OPTIONAL {
                 ?uri toezicht:sessionDate ?besluitdatum .
             }  

         }

         GRAPH <http://mu.semte.ch/graphs/public> {
             ?bestuurseenheid skos:prefLabel ?naam;
                               ext:kbonummer ?kbonummer;
                               besluit:classificatie ?classificatie;
                               mu:uuid ?groupId .
              ?classificatie skos:prefLabel ?classificatieNaam.
              ?besluitType skos:prefLabel ?besluitTypeLabel .
         }

         FILTER(?graph = IRI(CONCAT("http://mu.semte.ch/graphs/organizations/", ?groupId, "/LoketLB-toezichtGebruiker")))

      } ORDER BY ASC(?modified)
`);
  return parseResult(result);
};

/**
 * cleanup running tasks
 */
const cleanup = async function() {
  await update(`
     PREFIX toezicht: <http://mu.semte.ch/vocabularies/ext/supervision/>

     DELETE {
       GRAPH ?g {
           ?report toezicht:status ${sparqlEscapeUri(STATUS_PROCESSING)}.
       }
     } WHERE {
       GRAPH ?g {
           ?report a toezicht:InzendingVoorToezicht;
                     toezicht:status ${sparqlEscapeUri(STATUS_PROCESSING)}.
       }
     }
  `);
};

/**
 * is a packaging task already return
 * @return {boolean} Whether a packaging task is currently running
 */
async function isRunning() {
  const queryResult = await query(`
     PREFIX toezicht: <http://mu.semte.ch/vocabularies/ext/supervision/>

     ASK {
       GRAPH ?g {
         ?report a toezicht:InzendingVoorToezicht;
                 toezicht:status ${sparqlEscapeUri(STATUS_PROCESSING)}.
       }
     }`);
  return queryResult.boolean;
}

export {
  isRunning,
  cleanup,
  addPackage,
  generateZipFileName,
  createZipFile,
  createBorderel,
  createPublicatie,
  updateInternalDossierStatus,
  fetchFinancialDossiersToBePackaged,
  fetchFilesForDossier,
  STATUS_PROCESSING,
  STATUS_PACKAGED,
  STATUS_PACKAGING_FAILED
};
