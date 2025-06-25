import { readdir, stat } from 'fs/promises';
import { join, extname } from 'path';
import { spawn } from 'child_process';

const examplesDir = process.cwd();

async function runCommand(command, args) {
    return new Promise((resolve, reject) => {
        console.log(`\n> ${command} ${args.join(' ')}`);
        const child = spawn(command, args, { stdio: 'inherit', shell: true });

        child.on('close', (code) => {
            if (code === 0) {
                resolve();
            } else {
                reject(new Error(`Command failed with exit code ${code}`));
            }
        });

        child.on('error', (err) => {
            reject(err);
        });
    });
}

async function findAndRunExamples() {
    console.log('Starting to run all Iggy Node.js examples...');
    const files = await readdir(examplesDir);
    const exampleDirs = [];

    for (const file of files) {
        const fullPath = join(examplesDir, file);
        const stats = await stat(fullPath);
        if (stats.isDirectory()) {
            exampleDirs.push(file);
        }
    }

    for (const dir of exampleDirs) {
        const jsExamplePath = join(dir, 'main.js');
        const tsExamplePath = join(dir, 'main.ts');

        try {
            await stat(join(examplesDir, jsExamplePath));
            await runCommand('npm', ['start', '--', jsExamplePath]);
        } catch (e) {
            // JS file doesn't exist, do nothing
        }

        try {
            await stat(join(examplesDir, tsExamplePath));
            await runCommand('npm', ['run', 'start:ts', '--', tsExamplePath]);
        } catch (e) {
            // TS file doesn't exist, do nothing
        }
    }

    console.log('\nAll examples ran successfully!');
}

findAndRunExamples().catch(err => {
    console.error('\nAn error occurred while running examples:', err.message);
    process.exit(1);
});
