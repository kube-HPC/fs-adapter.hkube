const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
const { expect } = chai;
const moment = require('moment');
const path = require('path');
const fs = require('fs-extra');
const { Encoding, EncodingTypes } = require('@hkube/encoding');
const baseDir = 'storage/nfs/test/';
const uuid = require('uuid/v4');
const FsAdapter = require('../lib/fs-adapter');
const DIR_NAMES = {
    HKUBE: 'data/hkube',
    HKUBE_RESULTS: 'data/hkube-results',
    HKUBE_METADATA: 'data/hkube-metadata',
    HKUBE_STORE: 'data/hkube-store',
    HKUBE_EXECUTION: 'data/hkube-execution'
};
const DateFormat = 'YYYY-MM-DD';
const adapter = new FsAdapter();
const encoding = new Encoding({ type: 'json' });


describe('fs-adapter', () => {
    before(async () => {
        const options = {
            baseDirectory: baseDir
        };
        await adapter.init(options, DIR_NAMES, true);

        const wrapperGet = (fn) => {
            const wrapper = async (args) => {
                const result = await fn(args);
                return encoding.decode(result);
            }
            return wrapper;
        }

        const wrapperPut = (fn) => {
            const wrapper = (args) => {
                const data = encoding.encode(args.data);
                return fn({ ...args, data });
            }
            return wrapper;
        }

        adapter.put = wrapperPut(adapter.put.bind(adapter));
        adapter.get = wrapperGet(adapter.get.bind(adapter));
    });
    describe('put', () => {
        it.skip('put and get same value', async () => {
            const res = await adapter.get({ path: 'file1' });
            expect(res).to.equal('test');
        });
        it('put and get same value', async () => {
            const jobId = uuid();
            const link = await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), data: 'test' });
            const res = await adapter.get(link);
            expect(res).to.equal('test');
        });
        it('put and get same value', async () => {
            const jobId = uuid();
            const link = await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), data: 'test' });
            const res = await adapter.getMetadata(link);
            expect(res.size).to.equal(6);
        });
        it('put and get results same value', async () => {
            const link = await adapter.put({ path: path.join(DIR_NAMES.HKUBE_RESULTS, moment().format(DateFormat), uuid()), data: 'test-result' });
            const res = await adapter.get(link);
            expect(res).to.equal('test-result');
        });
        it('put and get results null', async () => {
            const link = await adapter.put({ path: path.join(DIR_NAMES.HKUBE_RESULTS, moment().format(DateFormat), uuid()), data: null });
            const res = await adapter.get(link);
            expect(res).to.equal(null);
        });
        it('put and get results []', async () => {
            const link = await adapter.put({ path: path.join(DIR_NAMES.HKUBE_RESULTS, moment().format(DateFormat), uuid()), data: [] });
            const res = await adapter.get(link);
            expect(res).to.deep.equal([]);
        });
        it('put and get results 33', async () => {
            const link = await adapter.put({ path: path.join(DIR_NAMES.HKUBE_RESULTS, moment().format(DateFormat), uuid()), data: 33 });
            const res = await adapter.get(link);
            expect(res).to.equal(33);
        });
        it('put and delete', async () => {
            const link = await adapter.put({ path: path.join(DIR_NAMES.HKUBE_RESULTS, moment().format(DateFormat), uuid()), data: 33 });
            const res = await fs.pathExists(path.join(baseDir, link.path));
            expect(res).to.equal(true);

            await adapter.delete(link);
            const res1 = await fs.pathExists(path.join(baseDir, link.path));
            expect(res1).to.equal(false);
        });
        it('put and list', async () => {
            const jobId = uuid();
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), data: 'test1' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), data: 'test2' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), data: 'test3' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), data: 'test4' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), data: 'test5' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), data: 'test6' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), data: 'test7' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), data: 'test8' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), data: 'test9' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), data: 'test10' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), data: 'test11' });

            const res = await adapter.list({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId) });
            expect(res.length).to.equal(11);
        });
        it('put and list with prefix', async () => {
            const jobId = uuid();
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, 'test1' + uuid()), data: 'test1' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, 'test2' + uuid()), data: 'test2' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, 'test3' + uuid()), data: 'test3' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, 'test4' + uuid()), data: 'test4' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, 'test5' + uuid()), data: 'test5' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, 'test6' + uuid()), data: 'test6' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, 'test7' + uuid()), data: 'test7' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, 'test8' + uuid()), data: 'test8' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, 'test9' + uuid()), data: 'test9' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, 'test10' + uuid()), data: 'test10' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, 'test11' + uuid()), data: 'test11' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, 'xx' + uuid()), data: 'test11' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, 'dd' + uuid()), data: 'test11' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, 'ff' + uuid()), data: 'test11' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, 'gg' + uuid()), data: 'test11' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, 'f1', 'xx' + uuid()), data: 'test11' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, 'f1', 'dd' + uuid()), data: 'test11' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, 'f1', 'ff' + uuid()), data: 'test11' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, 'f1', 'gg' + uuid()), data: 'test11' });
            const res = await adapter.list({ path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, 'test') });
            expect(res.length).to.equal(11);
        });
        it('put and list prefix', async () => {
            const jobId = uuid();
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment('2015-01-27').format(DateFormat), jobId, 'test1' + uuid()), data: 'test1' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment('2015-01-26').format(DateFormat), jobId, 'test2' + uuid()), data: 'test2' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment('2015-01-25').format(DateFormat), jobId, 'test3' + uuid()), data: 'test3' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment('2015-01-24').format(DateFormat), jobId, 'test4' + uuid()), data: 'test4' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment('2015-01-23').format(DateFormat), jobId, 'test5' + uuid()), data: 'test5' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment('2015-01-22').format(DateFormat), jobId, 'test6' + uuid()), data: 'test6' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, moment('2015-01-21').format(DateFormat), jobId, 'test7' + uuid()), data: 'test7' });

            const res = await adapter.listPrefixes({ path: DIR_NAMES.HKUBE });
            expect(res.includes('2015-01-27')).to.be.true;
            expect(res.includes('2015-01-26')).to.be.true;
            expect(res.includes('2015-01-25')).to.be.true;
            expect(res.includes('2015-01-24')).to.be.true;
            expect(res.includes('2015-01-23')).to.be.true;
            expect(res.includes('2015-01-22')).to.be.true;
            expect(res.includes('2015-01-21')).to.be.true;
        }).timeout(50000);
        it('list and get', async () => {
            const jobId = uuid();
            const folder = uuid();
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, folder, jobId, 'test1' + uuid()), data: 'test1' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, folder, jobId, 'test2' + uuid()), data: 'test2' });

            const res = await adapter.list({ path: path.join(DIR_NAMES.HKUBE, folder) });
            expect(res.length).to.eql(2);
            const file1 = await adapter.get(res[0]);
            expect(file1).to.exist;
            const file2 = await adapter.get(res[1]);
            expect(file2).to.exist;
        }).timeout(50000);
        it('list and get prefix', async () => {
            const jobId = uuid();
            const folder = uuid();
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, folder, jobId, 'test1' + uuid()), data: 'test1' });
            await adapter.put({ path: path.join(DIR_NAMES.HKUBE, folder, jobId, 'test2' + uuid()), data: 'test2' });

            const res = await adapter.list({ path: path.join(DIR_NAMES.HKUBE, folder, jobId, 'te') });
            expect(res.length).to.eql(2);
            const file1 = await adapter.get(res[0]);
            expect(file1).to.exist;
            const file2 = await adapter.get(res[1]);
            expect(file2).to.exist;
        }).timeout(50000);
    });
    describe('seek', () => {
        it('seek start: 0', async () => {
            const jobId = uuid();
            const folder = uuid();
            const data = 'my-new-value';
            const filePath = path.join(DIR_NAMES.HKUBE, folder, jobId);
            await adapter.put({ path: filePath, data });
            const options = {
                start: 0,
                end: null,
                path: filePath
            }
            const buffer = await adapter.seek(options);
            const res = buffer.toString('utf8');
            expect(res).to.equal(encoding.encode(data));
        });
        it('seek start: 0 end: 0', async () => {
            const jobId = uuid();
            const folder = uuid();
            const data = 'my-new-value';
            const filePath = path.join(DIR_NAMES.HKUBE, folder, jobId);
            await adapter.put({ path: filePath, data });
            const options = {
                start: 0,
                end: 0,
                path: filePath
            }
            const buffer = await adapter.seek(options);
            const res = buffer.toString('utf8');
            expect(res).to.equal("");
        });
        it('seek start: 0 end: 6', async () => {
            const jobId = uuid();
            const folder = uuid();
            const data = 'my-new-value';
            const filePath = path.join(DIR_NAMES.HKUBE, folder, jobId);
            await adapter.put({ path: filePath, data });
            const options = {
                start: 0,
                end: 6,
                path: filePath
            }
            const buffer = await adapter.seek(options);
            const res = buffer.toString('utf8');
            expect(res).to.equal(`"my-ne`);
        });
        it('seek end: -6', async () => {
            const jobId = uuid();
            const folder = uuid();
            const data = 'my-new-value';
            const filePath = path.join(DIR_NAMES.HKUBE, folder, jobId);
            await adapter.put({ path: filePath, data });
            const options = {
                start: null,
                end: -6,
                path: filePath
            }
            const buffer = await adapter.seek(options);
            const res = buffer.toString('utf8');
            expect(res).to.equal(`value"`);
        });
    });
    describe('Stream', () => {
        it('put and get results same value', async () => {
            return new Promise(async (resolve) => {
                const fileOut = path.join(adapter._baseDirectory, DIR_NAMES.HKUBE_STORE, 'stream-out.yml');
                const readStream = fs.createReadStream('tests/mocks/stream.yml');
                const fileInPath = path.join(DIR_NAMES.HKUBE_STORE, 'stream-in.yml');
                const link = await adapter.putStream({ path: fileInPath, data: readStream });
                const readable = await adapter.getStream(link);
                const writable = fs.createWriteStream(fileOut);
                const stream = readable.pipe(writable);
                stream.on('finish', () => {
                    const fileIn = path.join(adapter._baseDirectory, fileInPath);
                    const inFile = fs.readFileSync(fileIn, 'utf8');
                    const outFile = fs.readFileSync(fileOut, 'utf8');
                    expect(inFile).to.equal(outFile);
                    resolve();
                });
            });
        });
    });
    after(() => {
        Object.values(DIR_NAMES).forEach(dir => fs.removeSync(path.join(baseDir, dir)));
    });
});
