#!/bin/bash

if [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_BRANCH" == "master" ]; then

    echo -e "Publishing javadoc...\n"

    mkdir -p $HOME/javadoc-latest
    cp -R build/docs/javadoc/* $HOME/javadoc-latest


    mkdir -p $HOME/gh-pages
    cd $HOME/gh-pages
    git init
    git checkout --orphan gh-pages
    git remote add origin https://${GITHUB_TOKEN}@github.com/${TRAVIS_REPO_SLUG}

    cp -Rf $HOME/javadoc-latest/* ./
    git add -f .
    git commit -m "Javadoc from $TRAVIS_BRANCH on build $TRAVIS_BUILD_NUMBER auto-pushed to gh-pages"
    git push -fq origin gh-pages

    echo -e "Published Javadoc to gh-pages.\n"

fi