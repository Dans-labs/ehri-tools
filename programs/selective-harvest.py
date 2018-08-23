import sys
import os
import re
import argparse
from subprocess import run
import xml.etree.ElementTree as ET


VERBOSE = False

COMMAND = ('curl', '-s', '-o')

metadataPat = re.compile('<metadata[^>]*>(.*)</metadata>', re.S)
errorPat = re.compile('''<error.*code=['"]([^'"]*)['"][^>]*>(.*)</error>''', re.S)


def error(msg):
  if VERBOSE > 0:
    sys.stderr.write(msg)


def errorln(msg):
  if VERBOSE > 0:
    sys.stderr.write('{}\n'.format(msg))


def info(msg):
  if VERBOSE > 1:
    sys.stderr.write(msg)


def infoln(msg):
  if VERBOSE > 1:
    sys.stderr.write('{}\n'.format(msg))


def extra(msg):
  if VERBOSE > 2:
    sys.stderr.write(msg)


def extraln(msg):
  if VERBOSE > 2:
    sys.stderr.write('{}\n'.format(msg))


def readTask(config, selectRepos):
  if not os.path.exists(config):
    errorln('No config file "{}"'.format(config))
    return False
  info('Reading config file "{}" ...'.format(config))
  tree = ET.parse(config)
  infoln('done')

  repos = []

  root = tree.getroot()

  for rElem in root.iter('repository'):
    repoName = rElem.attrib['id']
    if selectRepos is not None and repoName not in selectRepos:
      infoln('skipping repo "{}"'.format(repoName))
      continue
    repoInfo = {
        'name': repoName,
        'sets': [],
    }
    for elem in rElem.findall('baseurl'):
      repoInfo['url'] = elem.text
    for elem in rElem.findall('metadataprefix'):
      repoInfo['meta'] = elem.text
    for elem in rElem.findall('recordpath'):
      repoInfo['dest'] = elem.text
    for elem in rElem.findall('output-set'):
      setInfo = {
          'name': elem.attrib['name'],
          'ids': set(),
      }
      for iElem in elem.findall('id'):
        setInfo['ids'].add(iElem.text)
      repoInfo['sets'].append(setInfo)
    repos.append(repoInfo)

  extraln(repos)
  return repos


def harvestAll(repoTasks):
  good = True
  for repoTask in repoTasks:
    thisGood = harvest(repoTask)
    if not thisGood:
      good = False
  return good


def harvest(repoTask):
  taskName = repoTask.get('name', 'UNSPECIFIED')
  infoln('Harvesting from "{}"'.format(taskName))
  dest = repoTask.get('dest', '')
  good = True
  if not os.path.exists(dest):
    try:
      os.makedirs(dest, exist_ok=True)
    except Exception:
      errorln('Cannot create directory "{}"'.format(dest))
      good = False
  else:
    if not os.path.isdir(dest):
      errorln('"{}" is not a directory'.format(dest))
      good = False
  if not good:
      return False

  for repoSet in repoTask.get('sets', []):
    setGood = True
    setName = repoSet.get('name', 'UNSPECIFIED')
    ids = sorted(repoSet.get('ids', []))
    infoln('\tHarvesting "{}" set "{}" with {} documents'.format(
        taskName, setName, len(ids),
    ))
    setDest = '{}/{}'.format(dest, setName)
    if not os.path.exists(setDest):
      try:
        os.makedirs(setDest, exist_ok=True)
      except Exception:
        errorln('Cannot create directory "{}"'.format(setDest))
        setGood = False
    else:
      if not os.path.isdir(setDest):
        errorln('"{}" is not a directory'.format(setDest))
        setGood = False
    if not setGood:
      good = False
      continue
    nError = 0
    for docId in ids:
      docError = None
      repoUrl = repoTask.get('url', '')
      meta = repoTask.get('meta', '')
      info('\t\tharvesting "{:<40}" ... '.format(docId))
      docUrl = '{}?verb=GetRecord&identifier={}&metadataPrefix={}'.format(
          repoUrl,
          docId,
          meta,
      )
      docDest = '{}/{}'.format(setDest, docId.replace(':', '-'))
      try:
        run(
            COMMAND + (docDest, docUrl)
        )
        error = deliver(docDest)
        if error is not None:
          docError = error
      except Exception as e:
        docError = e
      if docError and os.path.exists(docDest):
        os.unlink(docDest)
      if docError:
        setGood = False
        nError += 1
      infoln('XX' if docError else 'OK')
      if docError:
        docError = str(docError).rstrip('\n')
        infoln('\t\t\t{}'.format(docError))
    if not setGood:
      good = False
    infoln('\tHarvested "{}" set "{}" {} good, {} missed'.format(
        taskName, setName, len(ids) - nError, nError,
    ))
  return good


def deliver(path):
    with open(path) as fh:
      text = fh.read()
    error = None
    if '</GetRecord>' in text and '</metadata>' in text:
        match = metadataPat.search(text)
        if match:
            text = match.group(1).strip()
            with open(path, 'w') as fh:
              fh.write(text)
        else:
            error = 'No metadata found'
    elif '</error>' in text:
        match = errorPat.search(text)
        if match:
            code = match.group(1)
            msg = match.group(2)
            error = f'{code}: {msg}'
        else:
            error = 'Could not parse error message'
    else:
        error = 'No record found and no error message found'
    return error


def main():
  global VERBOSE

  parser = argparse.ArgumentParser(description='selective harvest arguments')
  parser.add_argument(
      '-c', '--config',
      default='config.xml',
      help='path to config file (xml)',
  )
  parser.add_argument(
      '-w', '--workdir',
      default='',
      help='path to working directory',
  )
  parser.add_argument(
      '-r', '--repo',
      default='',
      type=str,
      help='only do repos in comma separated list of repo ids',
  )
  parser.add_argument(
      '-v', '--verbose',
      action='count',
      default=0,
      help='print messages',
  )
  args = parser.parse_args()
  VERBOSE = args.verbose

  workDir = os.path.abspath(args.workdir)
  os.chdir(workDir)
  infoln('working in directory "{}"'.format(os.getcwd()))

  repos = None if args.repo == '' else set(args.repo.split(','))
  if repos is None:
    infoln('Harvest all repos found in "{}"'.format(args.config))
  else:
    infoln('Harvest repositories "{}" only'.format('", "'.join(repos)))
  repoTasks = readTask(args.config, repos)
  if not repoTasks:
    return 1
  if not harvestAll(repoTasks):
    return 1
  return 0


sys.exit(main())
