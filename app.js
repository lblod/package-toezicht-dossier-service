import { CronJob } from 'cron';
import { app, uuid, errorHandler } from 'mu';
import {
  addPackage,
  generateZipFileName,
  createBorderel,
  createPublicatie,
  createZipFile,
  cleanup,
  isRunning,
  updateInternalDossierStatus,
  fetchFinancialDossiersToBePackaged,
  fetchFilesForDossier,
  STATUS_PROCESSING,
  STATUS_PACKAGED,
  STATUS_PACKAGING_FAILED
} from './support';
import request from 'request';

/** Schedule packaging cron job */
const cronFrequency = process.env.PACKAGE_CRON_PATTERN || '* */12 * * * *';

cleanup();

new CronJob(cronFrequency, function() {
  console.log(`Toezicht packaging triggered by cron job at ${new Date().toISOString()}`);
  request.post('http://localhost/package-toezicht-dossiers/');
}, null, true);


app.post('/package-toezicht-dossiers/', async function( req, res, next ) {
  try {
    if (await isRunning())
      return res.status(503).end();
    const dossiers = await fetchFinancialDossiersToBePackaged();
    if (dossiers.length == 0) {
      console.log(`No Toezicht dossiers found that need to be packaged`);
      return res.status(204).end();
    }
    console.log(`Found ${dossiers.length} Toezicht dossiers to package`);
    Promise.all(dossiers.map( async (dossier) => { // don't await this since packaging is async
      console.log(`Start packaging Toezicht dossier ${dossier.id} found in graph <${dossier.graph}>`);
      try {
        await updateInternalDossierStatus(dossier.uri, STATUS_PROCESSING, dossier.graph);
        const files = await fetchFilesForDossier(dossier.uri, dossier.graph);

        if (files.length) {
          const publicatie = await createPublicatie(dossier);
          const borderel = await createBorderel(dossier, files, publicatie);
          const zipUUID = uuid();
          const fileName = generateZipFileName(dossier, zipUUID);
          const zipFile = await createZipFile(fileName, files, borderel, publicatie);
          await addPackage(dossier.uri, zipFile, zipUUID, fileName, dossier.graph);
          await updateInternalDossierStatus(dossier.uri, STATUS_PACKAGED, dossier.graph);
          console.log(`Packaged Toezicht dossier ${dossier.id} successfully`);
        } else {
          console.log(`Failed to package Toezicht dossiers ${dossier.id}: at least 1 attached file is expected`);
          await updateInternalDossierStatus(dossier.uri, STATUS_PACKAGING_FAILED, dossier.graph);
        }
      } catch(err) {
        console.log(`Failed to package Toezicht dossier ${dossier.id}: ${err}`);
        await updateInternalDossierStatus(dossier.uri, STATUS_PACKAGING_FAILED, dossier.graph);
      }
    }));
    return res.status(202).send({status:202, title: 'processing'});
  }
  catch(e) {
    return next(new Error(e.message));
  }
});

app.use(errorHandler);
