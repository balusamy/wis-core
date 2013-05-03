#!/usr/bin/env python2

from xml.etree.ElementTree import iterparse, QName


NS = 'http://www.mediawiki.org/xml/export-0.8/'
def tag_ns(tag):
    return str(QName(NS, tag))
tag_page     = tag_ns('page')
tag_title    = tag_ns('title')
tag_revision = tag_ns('revision')
tag_text     = tag_ns('text')
tag_sha1     = tag_ns('sha1')


def articles(stream):
    context = iter(iterparse(stream, events=('start', 'end')))
    _, root = context.next()

    for event, elem in context:
        if event == 'end' and elem.tag == tag_page:
            title = elem.find(tag_title).text
            rev = elem.find(tag_revision)
            text = rev.find(tag_text).text
            sha1 = rev.find(tag_sha1).text
            yield (title, sha1, text)

            root.clear()
