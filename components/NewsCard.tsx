import React from 'react';
import ReactMarkdown from 'react-markdown';

interface NewsCardProps {
  content: string;
}

const NewsCard: React.FC<NewsCardProps> = ({ content }) => {
  return (
    <div className="prose prose-invert prose-sm max-w-none text-gray-300 font-sans">
      <ReactMarkdown
        components={{
          h2: ({node, ...props}) => <h2 className="text-lg font-black text-white mt-6 mb-3 uppercase tracking-wider" {...props} />,
          h3: ({node, ...props}) => <h3 className="text-base font-bold text-white mt-4 mb-2 flex items-center gap-2" {...props} />,
          strong: ({node, ...props}) => <strong className="text-accent font-semibold" {...props} />,
          ul: ({node, ...props}) => <ul className="list-disc pl-4 space-y-2 my-4 marker:text-accent" {...props} />,
          li: ({node, ...props}) => <li className="text-gray-300/90 leading-relaxed" {...props} />,
          p: ({node, ...props}) => <p className="leading-relaxed mb-4 text-secondary" {...props} />,
        }}
      >
        {content}
      </ReactMarkdown>
    </div>
  );
};

export default NewsCard;